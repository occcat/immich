import {
  AssetMediaStatus,
  AssetUploadAction,
  AssetVisibility,
  checkBulkUpload,
  getBaseUrl,
  type AssetMediaResponseDto,
} from '@immich/sdk';
import { toastManager } from '@immich/ui';
import { tick } from 'svelte';
import { t } from 'svelte-i18n';
import { get } from 'svelte/store';
import { authManager } from '$lib/managers/auth-manager.svelte';
import { uploadManager } from '$lib/managers/upload-manager.svelte';
import { addAssetsToAlbums } from '$lib/services/album.service';
import { uploadAssetsStore } from '$lib/stores/upload';
import { UploadState } from '$lib/types';
import { sendRequest, uploadRequest } from '$lib/utils';
import { ExecutorQueue } from '$lib/utils/executor-queue';
import { asQueryString } from '$lib/utils/shared-links';
import { handleError } from './handle-error';

export const uploadExecutionQueue = new ExecutorQueue({ concurrency: 2 });

type FilePickerParam = { multiple?: boolean; extensions?: string[] };
type FileUploadParam = { multiple?: boolean; albumId?: string };

export const openFilePicker = async (options: FilePickerParam = {}) => {
  const { multiple = true, extensions } = options;

  return new Promise<File[]>((resolve, reject) => {
    try {
      const fileSelector = document.createElement('input');

      fileSelector.type = 'file';
      fileSelector.multiple = multiple;

      if (extensions) {
        fileSelector.accept = extensions.join(',');
      }

      fileSelector.addEventListener(
        'change',
        (e: Event) => {
          fileSelector.remove();

          const target = e.target as HTMLInputElement;
          if (!target.files) {
            return;
          }

          const files = Array.from(target.files);
          resolve(files);
        },
        { passive: true },
      );

      fileSelector.addEventListener('cancel', () => fileSelector.remove(), { passive: true });

      // Safari requires the file selector to be mounted
      fileSelector.hidden = true;
      document.body.append(fileSelector);
      fileSelector.click();
    } catch (error) {
      console.log('Error selecting file', error);
      reject(error);
    }
  });
};

export const openFileUploadDialog = async (options: FileUploadParam = {}) => {
  const { albumId, multiple = true } = options;
  const extensions = uploadManager.getExtensions();
  const files = await openFilePicker({
    multiple,
    extensions,
  });

  return fileUploadHandler({ files, albumId });
};

type FileUploadHandlerParams = Omit<FileUploaderParams, 'deviceAssetId' | 'assetFile'> & {
  files: File[];
};

export const fileUploadHandler = async ({
  files,
  albumId,
  isLockedAssets = false,
}: FileUploadHandlerParams): Promise<string[]> => {
  const extensions = uploadManager.getExtensions();
  const promises = [];
  for (const file of files) {
    const name = file.name.toLowerCase();
    if (extensions.some((extension) => name.endsWith(extension))) {
      const deviceAssetId = getDeviceAssetId(file);
      uploadAssetsStore.addItem({ id: deviceAssetId, file, albumId });
      promises.push(
        uploadExecutionQueue.addTask(() => fileUploader({ deviceAssetId, assetFile: file, albumId, isLockedAssets })),
      );
    } else {
      toastManager.warning(get(t)('unsupported_file_type', { values: { file: file.name, type: file.type } }), {
        timeout: 10_000,
      });
    }
  }

  const results = await Promise.all(promises);
  return results.filter((result): result is string => !!result);
};

function getDeviceAssetId(asset: File) {
  return 'web-' + asset.name + '-' + asset.lastModified;
}

function hashFile(file: File): Promise<string> {
  return new Promise<string>((resolve, reject) => {
    const worker = new Worker(new URL('$lib/workers/hash-file.ts', import.meta.url), { type: 'module' });

    worker.addEventListener('message', ({ data }: MessageEvent<{ result?: string; error?: string }>) => {
      worker.terminate();

      if (data.error) {
        reject(new Error(data.error));
      } else {
        resolve(data.result!);
      }
    });

    worker.addEventListener('error', (event) => {
      worker.terminate();

      reject(new Error(event.message));
    });

    worker.postMessage(file);
  });
}

type FileUploaderParams = {
  assetFile: File;
  albumId?: string;
  replaceAssetId?: string;
  isLockedAssets?: boolean;
  // TODO rework the asset uploader and remove this
  deviceAssetId: string;
};

const MIN_UPLOAD_CHUNK_BYTES = 5 * 1024 * 1024;

type UploadBody = { id?: string; status?: AssetMediaStatus; isTrashed?: boolean };

const quoteStructured = (value: string) => `"${value.replaceAll('\\', '\\\\').replaceAll('"', String.raw`\"`)}"`;

const hexToBase64 = (hex: string) => {
  const bytes = hex.match(/.{2}/g)?.map((byte) => Number.parseInt(byte, 16)) ?? [];
  return btoa(bytes.map((byte) => String.fromCodePoint(byte)).join(''));
};

const readMaxAppendSize = async (uploadUrl: string) => {
  try {
    const response = await sendRequest<unknown>({ url: uploadUrl, method: 'OPTIONS' });
    if (!response.ok) {
      return undefined;
    }
    const match = /(?:^|,\s*)max-append-size=(\d+)/.exec(response.headers['upload-limit'] ?? '');
    const limit = match ? Number(match[1]) : 0;
    return limit > 0 ? limit : undefined;
  } catch {
    return undefined;
  }
};

const uploadWithRufh = async ({
  assetFile,
  deviceAssetId,
  fileCreatedAt,
  checksum,
  onProgress,
}: {
  assetFile: File;
  deviceAssetId: string;
  fileCreatedAt: string;
  checksum?: string;
  onProgress: (loaded: number, total: number) => void;
}): Promise<UploadBody | undefined> => {
  if (assetFile.size <= MIN_UPLOAD_CHUNK_BYTES) {
    return undefined;
  }

  const query = asQueryString(authManager.params);
  const withQuery = (url: string) => (query ? `${url}${url.includes('?') ? '&' : '?'}${query}` : url);
  const uploadUrl = withQuery(getBaseUrl() + '/upload');
  let chunkSize = await readMaxAppendSize(uploadUrl);
  if (!chunkSize || assetFile.size <= chunkSize) {
    return undefined;
  }

  const digest = hexToBase64(checksum ?? (await hashFile(assetFile)));
  const assetData = [
    `device-asset-id=${quoteStructured(deviceAssetId)}`,
    `device-id=${quoteStructured('web')}`,
    `filename=${quoteStructured(assetFile.name)}`,
    `file-created-at=${quoteStructured(fileCreatedAt)}`,
    `file-modified-at=${quoteStructured(fileCreatedAt)}`,
    'is-favorite=?0',
  ].join(', ');

  let offset = 0;
  let location = uploadUrl;
  let stalls = 0;
  const size = assetFile.size;

  while (offset < size) {
    const end = Math.min(offset + chunkSize, size);
    const isComplete = end === size;
    const headers: Record<string, string> = {
      'Upload-Draft-Interop-Version': '6',
      'Upload-Complete': isComplete ? '?1' : '?0',
      'Upload-Length': String(size),
      'Content-Type': offset === 0 ? 'application/octet-stream' : 'application/partial-upload',
    };
    if (offset === 0) {
      headers['Repr-Digest'] = `sha=:${digest}:`;
      headers['x-immich-asset-data'] = assetData;
    } else {
      headers['Upload-Offset'] = String(offset);
    }

    let response: Awaited<ReturnType<typeof sendRequest<UploadBody>>>;
    try {
      response = await sendRequest<UploadBody>({
        url: location,
        method: offset === 0 ? 'POST' : 'PATCH',
        headers,
        data: assetFile.slice(offset, end),
        onUploadProgress: (event) => onProgress(offset + event.loaded, size),
      });
    } catch {
      stalls += 1;
      if (stalls > 5) {
        throw new Error('Upload interrupted');
      }
      continue;
    }

    if (response.status === 413) {
      const match = /(?:^|,\s*)max-append-size=(\d+)/.exec(response.headers['upload-limit'] ?? '');
      const serverMax = match ? Number(match[1]) : 0;
      const next =
        serverMax > 0 && serverMax < chunkSize
          ? serverMax
          : Math.max(MIN_UPLOAD_CHUNK_BYTES, Math.floor(chunkSize / 2));
      if (next >= end - offset) {
        throw new Error('Upload chunk exceeds the server limit');
      }
      chunkSize = next;
      continue;
    }

    if (response.status === 409) {
      const serverOffset = Number(response.headers['upload-offset']);
      if (!Number.isSafeInteger(serverOffset) || serverOffset === offset) {
        stalls += 1;
        if (stalls > 5) {
          throw new Error('Upload offset did not advance');
        }
        continue;
      }
      offset = serverOffset;
      stalls = 0;
      continue;
    }

    if (!response.ok) {
      throw new Error('Unable to upload file');
    }

    stalls = 0;
    if (response.data.id && (isComplete || response.data.status)) {
      onProgress(size, size);
      return {
        id: response.data.id,
        status: response.data.status ?? AssetMediaStatus.Created,
        isTrashed: response.data.isTrashed,
      };
    }

    const nextLocation = response.headers.location;
    if (offset === 0 && nextLocation) {
      location = withQuery(new URL(nextLocation, globalThis.location.origin).href);
    }
    const reportedOffset = Number(response.headers['upload-offset']);
    offset = Number.isSafeInteger(reportedOffset) ? reportedOffset : end;
    onProgress(offset, size);
  }

  throw new Error('Upload did not finish');
};

// TODO: should probably use the @api SDK
async function fileUploader({
  assetFile,
  deviceAssetId,
  albumId,
  isLockedAssets = false,
}: FileUploaderParams): Promise<string | undefined> {
  const fileCreatedAt = new Date(assetFile.lastModified).toISOString();
  const $t = get(t);
  const wasInitiallyLoggedIn = !!authManager.authenticated;

  uploadAssetsStore.markStarted(deviceAssetId);

  try {
    const formData = new FormData();
    for (const [key, value] of Object.entries({
      fileCreatedAt,
      fileModifiedAt: new Date(assetFile.lastModified).toISOString(),
      isFavorite: 'false',
      assetData: new File([assetFile], assetFile.name),
    })) {
      formData.append(key, value);
    }

    if (isLockedAssets) {
      formData.append('visibility', AssetVisibility.Locked);
    }

    let responseData: { id: string; status: AssetMediaStatus; isTrashed?: boolean } | undefined;
    let checksum: string | undefined;
    if (!authManager.isSharedLink) {
      uploadAssetsStore.updateItem(deviceAssetId, { message: $t('asset_hashing') });
      await tick();
      try {
        checksum = await hashFile(assetFile);

        const {
          results: [checkUploadResult],
        } = await checkBulkUpload({ assetBulkUploadCheckDto: { assets: [{ id: assetFile.name, checksum }] } });
        if (checkUploadResult.action === AssetUploadAction.Reject && checkUploadResult.assetId) {
          responseData = {
            status: AssetMediaStatus.Duplicate,
            id: checkUploadResult.assetId,
            isTrashed: checkUploadResult.isTrashed,
          };
        }
      } catch (error) {
        console.error(`Error calculating sha1 file=${assetFile.name})`, error);
      }
    }

    if (!responseData) {
      uploadAssetsStore.updateItem(deviceAssetId, { message: $t('asset_uploading') });
      const chunked = await uploadWithRufh({
        assetFile,
        deviceAssetId,
        fileCreatedAt,
        checksum,
        onProgress: (loaded, total) => uploadAssetsStore.updateProgress(deviceAssetId, loaded, total),
      });
      if (chunked?.id) {
        responseData = {
          id: chunked.id,
          status: chunked.status ?? AssetMediaStatus.Created,
          isTrashed: chunked.isTrashed,
        };
      }
    }

    if (!responseData) {
      const queryParams = asQueryString(authManager.params);
      const response = await uploadRequest<AssetMediaResponseDto>({
        url: getBaseUrl() + '/assets' + (queryParams ? `?${queryParams}` : ''),
        data: formData,
        onUploadProgress: (event) => uploadAssetsStore.updateProgress(deviceAssetId, event.loaded, event.total),
      });

      if (![200, 201].includes(response.status)) {
        throw new Error($t('errors.unable_to_upload_file'));
      }

      responseData = response.data;
    }

    if (responseData.status === AssetMediaStatus.Duplicate) {
      uploadAssetsStore.track('duplicate');
    } else {
      uploadAssetsStore.track('success');
    }

    if (albumId && !authManager.isSharedLink) {
      uploadAssetsStore.updateItem(deviceAssetId, { message: $t('asset_adding_to_album') });
      await addAssetsToAlbums([albumId], [responseData.id], { notify: false });
      uploadAssetsStore.updateItem(deviceAssetId, { message: $t('asset_added_to_album') });
    }

    uploadAssetsStore.updateItem(deviceAssetId, {
      state: responseData.status === AssetMediaStatus.Duplicate ? UploadState.DUPLICATED : UploadState.DONE,
      assetId: responseData.id,
      isTrashed: responseData.isTrashed,
    });

    if (responseData.status !== AssetMediaStatus.Duplicate) {
      setTimeout(() => {
        uploadAssetsStore.removeItem(deviceAssetId);
      }, 1000);
    }

    return responseData.id;
  } catch (error) {
    // If the user store no longer holds a user, it means they have logged out
    // In this case don't bother reporting any errors.
    if (wasInitiallyLoggedIn && !authManager.authenticated) {
      return;
    }

    const errorMessage = handleError(error, $t('errors.unable_to_upload_file'));
    uploadAssetsStore.track('error');
    uploadAssetsStore.updateItem(deviceAssetId, { state: UploadState.ERROR, error: errorMessage });
    return;
  }
}
