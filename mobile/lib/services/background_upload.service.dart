import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:background_downloader/background_downloader.dart';
import 'package:crypto/crypto.dart';
import 'package:flutter/foundation.dart';
import 'package:freezed_annotation/freezed_annotation.dart';
import 'package:hooks_riverpod/hooks_riverpod.dart';
import 'package:immich_mobile/constants/constants.dart';
import 'package:immich_mobile/domain/models/asset/asset_metadata.model.dart';
import 'package:immich_mobile/domain/models/asset/base_asset.model.dart';
import 'package:immich_mobile/domain/models/store.model.dart';
import 'package:immich_mobile/domain/services/asset.service.dart';
import 'package:immich_mobile/entities/store.entity.dart';
import 'package:immich_mobile/extensions/platform_extensions.dart';
import 'package:immich_mobile/infrastructure/repositories/backup.repository.dart';
import 'package:immich_mobile/infrastructure/repositories/local_asset.repository.dart';
import 'package:immich_mobile/infrastructure/repositories/settings.repository.dart';
import 'package:immich_mobile/infrastructure/repositories/storage.repository.dart';
import 'package:immich_mobile/providers/infrastructure/asset.provider.dart';
import 'package:immich_mobile/providers/infrastructure/db.provider.dart';
import 'package:immich_mobile/providers/infrastructure/storage.provider.dart';
import 'package:immich_mobile/repositories/asset_media.repository.dart';
import 'package:immich_mobile/repositories/upload.repository.dart';
import 'package:immich_mobile/services/api.service.dart';
import 'package:immich_mobile/utils/debug_print.dart';
import 'package:logging/logging.dart';
import 'package:openapi/api.dart' as api;
import 'package:path/path.dart' as p;

part 'background_upload.service.freezed.dart';

final backgroundUploadServiceProvider = Provider((ref) {
  final db = ref.watch(driftProvider);
  final service = BackgroundUploadService(
    ref.watch(uploadRepositoryProvider),
    ref.watch(storageRepositoryProvider),
    db.localAssetRepository,
    db.backupRepository,
    ref.watch(assetMediaRepositoryProvider),
    ref.watch(assetServiceProvider),
  );

  ref.onDispose(service.dispose);
  return service;
});

/// Metadata for upload tasks to track live photo handling
@Freezed(fromJson: false, toJson: false)
abstract class UploadTaskMetadata with _$UploadTaskMetadata {
  const UploadTaskMetadata._();

  const factory UploadTaskMetadata({
    required String localAssetId,
    required bool isLivePhotos,
    required String livePhotoVideoId,
    String? checksum,
  }) = _UploadTaskMetadata;

  Map<String, dynamic> toMap() {
    return <String, dynamic>{
      'localAssetId': localAssetId,
      'isLivePhotos': isLivePhotos,
      'livePhotoVideoId': livePhotoVideoId,
      'checksum': checksum,
    };
  }

  factory UploadTaskMetadata.fromMap(Map<String, dynamic> map) {
    return UploadTaskMetadata(
      localAssetId: map['localAssetId'] as String,
      isLivePhotos: map['isLivePhotos'] as bool,
      livePhotoVideoId: map['livePhotoVideoId'] as String,
      checksum: map['checksum'] as String?,
    );
  }

  factory UploadTaskMetadata.fromJson(String source) =>
      UploadTaskMetadata.fromMap(json.decode(source) as Map<String, dynamic>);

  String toJson() => json.encode(toMap());
}

/// Service for handling background uploads using iOS URLSession (background_downloader)
///
/// This service handles asynchronous background uploads that can continue
/// even when the app is suspended. Primarily used for iOS background backup.
class BackgroundUploadService {
  BackgroundUploadService(
    this._uploadRepository,
    this._storageRepository,
    this._localAssetRepository,
    this._backupRepository,
    this._assetMediaRepository,
    this._assetService,
  ) {
    _uploadRepository.onUploadStatus = _onUploadCallback;
    _uploadRepository.onTaskProgress = _onTaskProgressCallback;
  }

  final UploadRepository _uploadRepository;
  final StorageRepository _storageRepository;
  final LocalAssetRepository _localAssetRepository;
  final BackupRepository _backupRepository;
  final AssetMediaRepository _assetMediaRepository;
  final AssetService _assetService;
  final Logger _logger = Logger('BackgroundUploadService');

  final StreamController<TaskStatusUpdate> _taskStatusController = StreamController<TaskStatusUpdate>.broadcast();
  final StreamController<TaskProgressUpdate> _taskProgressController = StreamController<TaskProgressUpdate>.broadcast();

  // ignore: unused-code
  Stream<TaskStatusUpdate> get taskStatusStream => _taskStatusController.stream;
  // ignore: unused-code
  Stream<TaskProgressUpdate> get taskProgressStream => _taskProgressController.stream;

  bool shouldAbortQueuingTasks = false;

  void _onTaskProgressCallback(TaskProgressUpdate update) {
    if (!_taskProgressController.isClosed) {
      _taskProgressController.add(update);
    }
  }

  void _onUploadCallback(TaskStatusUpdate update) {
    if (!_taskStatusController.isClosed) {
      _taskStatusController.add(update);
    }
    unawaited(_handleTaskStatusUpdate(update));
  }

  void dispose() {
    unawaited(_taskStatusController.close());
    unawaited(_taskProgressController.close());
  }

  /// Enqueue tasks to the background upload queue
  Future<List<bool>> enqueueTasks(List<UploadTask> tasks) {
    return _uploadRepository.enqueueBackgroundAll(tasks);
  }

  /// Get a list of tasks that are ENQUEUED or RUNNING
  Future<List<Task>> getActiveTasks(String group) {
    return _uploadRepository.getActiveTasks(group);
  }

  /// Start background upload using iOS URLSession
  ///
  /// Finds backup candidates, builds upload tasks, and enqueues them
  /// for background processing.
  Future<void> uploadBackupCandidates(String userId) async {
    await _storageRepository.clearCache();
    shouldAbortQueuingTasks = false;

    final candidates = await _backupRepository.getCandidates(userId);
    if (candidates.isEmpty) {
      _logger.info("No new backup candidates found, finishing background upload");
      return;
    }

    _logger.info("Found ${candidates.length} backup candidates for background tasks");

    const batchSize = 100;
    final batch = candidates.take(batchSize).toList();
    final List<UploadTask> tasks = [];

    for (final asset in batch) {
      final task = await getUploadTask(asset);
      if (task != null) {
        tasks.add(task);
      }
    }

    if (tasks.isNotEmpty && !shouldAbortQueuingTasks) {
      _logger.info("Enqueuing ${tasks.length} background upload tasks");
      await enqueueTasks(tasks);
    }
  }

  /// Cancel all ongoing background uploads and reset the upload queue
  ///
  /// Returns the number of tasks left in the queue
  Future<int> cancel() async {
    shouldAbortQueuingTasks = true;

    await _storageRepository.clearCache();
    await _uploadRepository.reset(kBackupGroup);
    await _uploadRepository.deleteDatabaseRecords(kBackupGroup);

    final activeTasks = await _uploadRepository.getActiveTasks(kBackupGroup);
    return activeTasks.length;
  }

  /// Resume background backup processing
  Future<void> resume() {
    return _uploadRepository.start();
  }

  Future<void> _handleTaskStatusUpdate(TaskStatusUpdate update) async {
    if (_isRufh(update.task.metaData)) {
      await _handleRufhUpdate(update);
      return;
    }

    switch (update.status) {
      case TaskStatus.complete:
        unawaited(_handleLivePhoto(update));
        unawaited(_stackEditedAsset(update));

        if (CurrentPlatform.isIOS) {
          try {
            final path = await update.task.filePath();
            await File(path).delete();
          } catch (e) {
            _logger.severe('Error deleting file path for iOS: $e');
          }
        }

      default:
        break;
    }
  }

  Future<void> _handleLivePhoto(TaskStatusUpdate update) async {
    try {
      if (update.task.metaData.isEmpty || update.task.metaData == '') {
        return;
      }

      final metadata = UploadTaskMetadata.fromJson(update.task.metaData);
      if (!metadata.isLivePhotos) {
        return;
      }

      if (update.responseBody == null || update.responseBody!.isEmpty) {
        return;
      }
      final response = jsonDecode(update.responseBody!);

      final localAsset = await _localAssetRepository.getById(metadata.localAssetId);
      if (localAsset == null) {
        return;
      }

      final uploadTask = await getLivePhotoUploadTask(localAsset, response['id'] as String);

      if (uploadTask == null) {
        return;
      }

      await enqueueTasks([uploadTask]);
    } catch (error, stackTrace) {
      dPrint(() => "Error handling live photo upload task: $error $stackTrace");
    }
  }

  Future<void> _stackEditedAsset(TaskStatusUpdate update) async {
    try {
      if (update.task.metaData.isEmpty || update.responseBody == null || update.responseBody!.isEmpty) {
        return;
      }

      final metadata = UploadTaskMetadata.fromJson(update.task.metaData);
      // The video half of a live photo carries isLivePhotos; the still that follows is the final asset
      if (metadata.isLivePhotos) {
        return;
      }

      await _assetService.stackEditedUpload(
        metadata.localAssetId,
        jsonDecode(update.responseBody!)['id'] as String,
        metadata.checksum,
      );
    } catch (error, stackTrace) {
      dPrint(() => "Error stacking edited asset upload: $error $stackTrace");
    }
  }

  @visibleForTesting
  Future<UploadTask?> getUploadTask(LocalAsset asset, {String group = kBackupGroup, int? priority}) async {
    final entity = await _storageRepository.getAssetEntityForAsset(asset);
    if (entity == null) {
      _logger.warning("Asset entity not found for ${asset.id} - ${asset.name}");
      return null;
    }

    File? file;

    /// iOS LivePhoto has two files: a photo and a video.
    /// They are uploaded separately, with video file being upload first, then returned with the assetId
    /// The assetId is then used as a metadata for the photo file upload task.
    ///
    /// We implement two separate upload groups for this, the normal one for the video file
    /// and the higher priority group for the photo file because the video file is already uploaded.
    ///
    /// The cancel operation will only cancel the video group (normal group), the photo group will not
    /// be touched, as the video file is already uploaded.

    if (entity.isLivePhoto) {
      file = await _storageRepository.getMotionFileForAsset(asset);
    } else {
      file = await _storageRepository.getFileForAsset(asset.id);
    }

    if (file == null) {
      _logger.warning("Failed to get file for asset ${asset.id} - ${asset.name}");
      return null;
    }

    final fileName = await _assetMediaRepository.getOriginalFilename(asset.id) ?? asset.name;
    // Some apps (e.g. DJI/Fusion) return names without an extension; fall back to the asset name for those.
    final extension = p.extension(file.path).isNotEmpty ? p.extension(file.path) : p.extension(asset.name);
    final originalFileName = p.setExtension(fileName, extension);

    final String metadata = UploadTaskMetadata(
      localAssetId: asset.id,
      isLivePhotos: entity.isLivePhoto,
      livePhotoVideoId: '',
      checksum: asset.checksum,
    ).toJson();

    final requiresWiFi = _shouldRequireWiFi(asset);

    return buildUploadTask(
      file,
      createdAt: asset.createdAt,
      modifiedAt: asset.updatedAt,
      originalFileName: originalFileName,
      deviceAssetId: asset.id,
      metadata: metadata,
      group: group,
      priority: priority,
      isFavorite: asset.isFavorite,
      requiresWiFi: requiresWiFi,
      // Visibility hidden on upload to prevent the server from running regular jobs on the live photo asset
      fields: entity.isLivePhoto ? {'visibility': api.AssetVisibility.hidden.toString()} : null,
      cloudId: entity.isLivePhoto ? null : asset.cloudId,
      adjustmentTime: entity.isLivePhoto ? null : asset.adjustmentTime?.toIso8601String(),
      latitude: entity.isLivePhoto ? null : asset.latitude?.toString(),
      longitude: entity.isLivePhoto ? null : asset.longitude?.toString(),
    );
  }

  @visibleForTesting
  Future<UploadTask?> getLivePhotoUploadTask(LocalAsset asset, String livePhotoVideoId) async {
    final entity = await _storageRepository.getAssetEntityForAsset(asset);
    if (entity == null) {
      return null;
    }

    final file = await _storageRepository.getFileForAsset(asset.id);
    if (file == null) {
      return null;
    }

    final fields = {'livePhotoVideoId': livePhotoVideoId};

    final requiresWiFi = _shouldRequireWiFi(asset);
    final originalFileName = await _assetMediaRepository.getOriginalFilename(asset.id) ?? asset.name;

    return buildUploadTask(
      file,
      createdAt: asset.createdAt,
      modifiedAt: asset.updatedAt,
      originalFileName: originalFileName,
      deviceAssetId: asset.id,
      metadata: UploadTaskMetadata(
        localAssetId: asset.id,
        isLivePhotos: false,
        livePhotoVideoId: livePhotoVideoId,
        checksum: asset.checksum,
      ).toJson(),
      fields: fields,
      group: kBackupLivePhotoGroup,
      priority: 0, // Highest priority to get upload immediately
      isFavorite: asset.isFavorite,
      requiresWiFi: requiresWiFi,
      cloudId: asset.cloudId,
      adjustmentTime: asset.adjustmentTime?.toIso8601String(),
      latitude: asset.latitude?.toString(),
      longitude: asset.longitude?.toString(),
    );
  }

  bool _shouldRequireWiFi(LocalAsset asset) {
    final backup = SettingsRepository.instance.appConfig.backup;
    if (asset.isVideo && backup.useCellularForVideos) {
      return false;
    }
    if (!asset.isVideo && backup.useCellularForPhotos) {
      return false;
    }
    return true;
  }

  Future<UploadTask> buildUploadTask(
    File file, {
    required String group,
    required DateTime createdAt,
    required DateTime modifiedAt,
    Map<String, String>? fields,
    String? originalFileName,
    String? deviceAssetId,
    String? metadata,
    int? priority,
    bool? isFavorite,
    bool requiresWiFi = true,
    String? cloudId,
    String? adjustmentTime,
    String? latitude,
    String? longitude,
  }) async {
    final serverEndpoint = Store.get(StoreKey.serverEndpoint);
    final url = Uri.parse('$serverEndpoint/assets').toString();
    final headers = ApiService.getRequestHeaders();
    final deviceId = Store.get(StoreKey.deviceId);
    final (baseDirectory, directory, filename) = await Task.split(filePath: file.path);
    final length = _fileLength(file);
    if (length != null && length > kUploadChunkFloorBytes) {
      final limit = await _uploadRepository.maxAppendBytes();
      if (limit != null && length > limit) {
        final digest = base64Encode((await sha1.bind(file.openRead()).first).bytes);
        return _rufhUploadTask(
          fileLength: length,
          chunkSize: limit,
          checksum: digest,
          offset: 0,
          url: Uri.parse('$serverEndpoint/upload').toString(),
          baseDirectory: baseDirectory,
          directory: directory,
          filename: filename,
          originalFileName: originalFileName,
          deviceAssetId: deviceAssetId,
          metadata: metadata,
          group: group,
          priority: priority,
          requiresWiFi: requiresWiFi,
          fields: {
            'filename': originalFileName ?? filename,
            'deviceAssetId': deviceAssetId ?? '',
            'deviceId': deviceId,
            'fileCreatedAt': createdAt.toUtc().toIso8601String(),
            'fileModifiedAt': modifiedAt.toUtc().toIso8601String(),
            'isFavorite': isFavorite?.toString() ?? 'false',
            if (cloudId != null && cloudId.isNotEmpty) 'icloudId': cloudId,
            ...?fields,
          },
        );
      }
    }
    final fieldsMap = {
      'filename': originalFileName ?? filename,
      // deviceAssetId/deviceId required by server v2.7.5 and below (drop in v4.0 per #27818).
      'deviceAssetId': deviceAssetId ?? '',
      'deviceId': deviceId,
      'fileCreatedAt': createdAt.toUtc().toIso8601String(),
      'fileModifiedAt': modifiedAt.toUtc().toIso8601String(),
      'isFavorite': isFavorite?.toString() ?? 'false',
      'duration': '0',
      ...?fields,
      if (CurrentPlatform.isIOS && cloudId != null)
        'metadata': jsonEncode([
          RemoteAssetMetadataItem(
            key: RemoteAssetMetadataKey.mobileApp,
            value: RemoteAssetMobileAppMetadata(
              cloudId: cloudId,
              createdAt: createdAt.toIso8601String(),
              adjustmentTime: adjustmentTime,
              latitude: latitude,
              longitude: longitude,
            ),
          ),
        ]),
    };

    return UploadTask(
      taskId: deviceAssetId,
      displayName: originalFileName ?? filename,
      httpRequestMethod: 'POST',
      url: url,
      headers: headers,
      filename: filename,
      fields: fieldsMap,
      baseDirectory: baseDirectory,
      directory: directory,
      fileField: 'assetData',
      metaData: metadata ?? '',
      group: group,
      requiresWiFi: requiresWiFi,
      priority: priority ?? 5,
      updates: Updates.statusAndProgress,
      retries: 3,
    );
  }

  int? _fileLength(File file) {
    try {
      return file.lengthSync();
    } catch (_) {
      return null;
    }
  }

  bool _isRufh(String metadata) {
    if (metadata.isEmpty) {
      return false;
    }
    try {
      final decoded = json.decode(metadata);
      return decoded is Map && decoded['uploadPhase'] == 'rufh';
    } catch (_) {
      return false;
    }
  }

  Future<void> _handleRufhUpdate(TaskStatusUpdate update) async {
    final task = update.task;
    if (task is! UploadTask) {
      return;
    }
    final meta = Map<String, dynamic>.from(json.decode(task.metaData) as Map);
    final size = (meta['uploadSize'] as num).toInt();
    var offset = (meta['uploadOffset'] as num).toInt();
    var chunkSize = (meta['uploadChunkSize'] as num).toInt();
    final attempts = (meta['uploadAttempts'] as num?)?.toInt() ?? 0;
    final willComplete = meta['uploadComplete'] == true;

    if (update.status == TaskStatus.complete && willComplete) {
      unawaited(_handleLivePhoto(update));
      unawaited(_stackEditedAsset(update));
      if (CurrentPlatform.isIOS) {
        try {
          await File(await task.filePath()).delete();
        } catch (error) {
          _logger.severe('Error deleting file path for iOS: $error');
        }
      }
      return;
    }

    if (update.status == TaskStatus.failed) {
      if (attempts >= 5) {
        _logger.warning('Resumable upload stopped after repeated failures');
        return;
      }
      final code = update.responseStatusCode;
      if (code == 413) {
        final serverMax = _maxAppendSize(update.responseHeaders?['upload-limit']);
        final halved = chunkSize ~/ 2 < kUploadChunkFloorBytes ? kUploadChunkFloorBytes : chunkSize ~/ 2;
        final next = serverMax != null && serverMax < chunkSize ? serverMax : halved;
        if (next >= chunkSize && offset + chunkSize <= size) {
          return;
        }
        chunkSize = next;
      } else if (code == 409) {
        final serverOffset = int.tryParse(update.responseHeaders?['upload-offset'] ?? '');
        if (serverOffset != null) {
          offset = serverOffset;
        }
      }
      meta['uploadAttempts'] = attempts + 1;
    } else if (update.status == TaskStatus.complete) {
      final location = update.responseHeaders?['location'];
      if (location != null && location.isNotEmpty && offset == 0) {
        meta['uploadUrl'] = _resolveLocation(task.url, location).toString();
      }
      final serverOffset = int.tryParse(update.responseHeaders?['upload-offset'] ?? '');
      offset = serverOffset ?? offset + chunkSize;
      if (offset > size) {
        offset = size;
      }
      meta['uploadAttempts'] = 0;
    } else {
      return;
    }

    if (offset >= size) {
      return;
    }

    meta['uploadOffset'] = offset;
    meta['uploadChunkSize'] = chunkSize;
    final end = offset + chunkSize < size ? offset + chunkSize : size;
    meta['uploadComplete'] = end == size;
    final next = task.copyWith(
      taskId: '${task.taskId}:$offset:$chunkSize:${meta['uploadAttempts']}',
      url: (meta['uploadUrl'] as String?) ?? task.url,
      httpRequestMethod: offset == 0 ? 'POST' : 'PATCH',
      post: 'binary',
      fields: const {},
      headers: _rufhHeaders(
        offset: offset,
        end: end,
        size: size,
        checksum: meta['uploadChecksum'] as String,
        assetData: meta['uploadAssetData'] as String,
        baseHeaders: ApiService.getRequestHeaders(),
      ),
      metaData: json.encode(meta),
      retries: 0,
    );
    await enqueueTasks([next]);
  }

  UploadTask _rufhUploadTask({
    required int fileLength,
    required int chunkSize,
    required String checksum,
    required int offset,
    required String url,
    required BaseDirectory baseDirectory,
    required String directory,
    required String filename,
    required String? originalFileName,
    required String? deviceAssetId,
    required String? metadata,
    required String group,
    required int? priority,
    required bool requiresWiFi,
    required Map<String, String> fields,
  }) {
    final end = chunkSize < fileLength ? chunkSize : fileLength;
    final assetData = _assetData(fields);
    final meta = metadata == null || metadata.isEmpty
        ? <String, dynamic>{}
        : Map<String, dynamic>.from(json.decode(metadata) as Map);
    meta.addAll({
      'uploadPhase': 'rufh',
      'uploadOffset': offset,
      'uploadSize': fileLength,
      'uploadChunkSize': chunkSize,
      'uploadChecksum': checksum,
      'uploadAssetData': assetData,
      'uploadAttempts': 0,
      'uploadComplete': end == fileLength,
      'uploadUrl': url,
    });

    return UploadTask(
      taskId: '${deviceAssetId ?? filename}:$offset',
      displayName: originalFileName ?? filename,
      httpRequestMethod: 'POST',
      post: 'binary',
      url: url,
      headers: _rufhHeaders(
        offset: offset,
        end: end,
        size: fileLength,
        checksum: checksum,
        assetData: assetData,
        baseHeaders: ApiService.getRequestHeaders(),
      ),
      filename: filename,
      fields: const {},
      baseDirectory: baseDirectory,
      directory: directory,
      metaData: json.encode(meta),
      group: group,
      requiresWiFi: requiresWiFi,
      priority: priority ?? 5,
      updates: Updates.statusAndProgress,
      retries: 0,
    );
  }

  Map<String, String> _rufhHeaders({
    required int offset,
    required int end,
    required int size,
    required String checksum,
    required String assetData,
    required Map<String, String> baseHeaders,
  }) {
    final headers = <String, String>{
      ...baseHeaders,
      'Range': 'bytes=$offset-${end - 1}',
      'Content-Type': offset == 0 ? 'application/octet-stream' : 'application/partial-upload',
      'Content-Disposition': '',
      'Upload-Draft-Interop-Version': '6',
      'Upload-Complete': end == size ? '?1' : '?0',
      'Upload-Length': '$size',
    };
    if (offset == 0) {
      headers['Repr-Digest'] = 'sha=:$checksum:';
      headers['x-immich-asset-data'] = assetData;
    } else {
      headers['Upload-Offset'] = '$offset';
    }
    return headers;
  }

  String _assetData(Map<String, String> fields) {
    String quote(String value) => '"${value.replaceAll('\\', '\\\\').replaceAll('"', '\\"')}"';
    final created = fields['fileCreatedAt'] ?? DateTime.now().toUtc().toIso8601String();
    final modified = fields['fileModifiedAt'] ?? created;
    final parts = <String>[
      'device-asset-id=${quote(fields['deviceAssetId'] ?? fields['filename'] ?? 'asset')}',
      'device-id=${quote(fields['deviceId'] ?? 'mobile')}',
      'filename=${quote(fields['filename'] ?? 'asset')}',
      'file-created-at=${quote(created)}',
      'file-modified-at=${quote(modified)}',
      'is-favorite=${fields['isFavorite'] == 'true' ? '?1' : '?0'}',
    ];
    final livePhotoVideoId = fields['livePhotoVideoId'];
    if (livePhotoVideoId != null && livePhotoVideoId.isNotEmpty) {
      parts.add('live-photo-video-id=${quote(livePhotoVideoId)}');
    }
    final cloudId = fields['icloudId'];
    if (cloudId != null && cloudId.isNotEmpty) {
      parts.add('icloud-id=${quote(cloudId)}');
    }
    return parts.join(', ');
  }

  int? _maxAppendSize(String? header) {
    final match = RegExp(r'(?:^|,\s*)max-append-size=(\d+)').firstMatch(header ?? '');
    return int.tryParse(match?.group(1) ?? '');
  }

  Uri _resolveLocation(String currentUrl, String location) {
    if (location.startsWith('http')) {
      return Uri.parse(location);
    }
    if (location.startsWith('/')) {
      return Uri.parse(currentUrl).replace(path: location);
    }
    return Uri.parse(currentUrl).resolve(location);
  }
}
