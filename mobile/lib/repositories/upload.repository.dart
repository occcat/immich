import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:background_downloader/background_downloader.dart';
import 'package:crypto/crypto.dart';
import 'package:hooks_riverpod/hooks_riverpod.dart';
import 'package:http/http.dart';
import 'package:immich_mobile/constants/constants.dart';
import 'package:immich_mobile/domain/models/store.model.dart';
import 'package:immich_mobile/entities/store.entity.dart';
import 'package:immich_mobile/infrastructure/repositories/network.repository.dart';
import 'package:logging/logging.dart';

final uploadRepositoryProvider = Provider((ref) => UploadRepository());

const int kUploadChunkFloorBytes = 5 * 1024 * 1024;

class UploadRepository {
  final Logger logger = Logger('UploadRepository');
  void Function(TaskStatusUpdate)? onUploadStatus;
  void Function(TaskProgressUpdate)? onTaskProgress;

  UploadRepository() {
    FileDownloader().registerCallbacks(
      group: kBackupGroup,
      taskStatusCallback: (update) => onUploadStatus?.call(update),
      taskProgressCallback: (update) => onTaskProgress?.call(update),
    );
    FileDownloader().registerCallbacks(
      group: kBackupLivePhotoGroup,
      taskStatusCallback: (update) => onUploadStatus?.call(update),
      taskProgressCallback: (update) => onTaskProgress?.call(update),
    );
    FileDownloader().registerCallbacks(
      group: kManualUploadGroup,
      taskStatusCallback: (update) => onUploadStatus?.call(update),
      taskProgressCallback: (update) => onTaskProgress?.call(update),
    );
  }

  Future<List<bool>> enqueueBackgroundAll(List<UploadTask> tasks) {
    return FileDownloader().enqueueAll(tasks);
  }

  Future<void> deleteDatabaseRecords(String group) {
    return FileDownloader().database.deleteAllRecords(group: group);
  }

  Future<int> reset(String group) {
    return FileDownloader().reset(group: group);
  }

  /// Get a list of tasks that are ENQUEUED or RUNNING
  Future<List<Task>> getActiveTasks(String group) {
    return FileDownloader().allTasks(group: group);
  }

  Future<void> start() {
    return FileDownloader().start();
  }

  Future<UploadResult> uploadFile({
    required File file,
    required String originalFileName,
    required Map<String, String> fields,
    required Completer<void>? cancelToken,
    void Function(int bytes, int totalBytes)? onProgress,
    required String logContext,
    Client? httpClient,
  }) async {
    final String savedEndpoint = Store.get(StoreKey.serverEndpoint);
    final length = _fileLength(file);
    if (length != null && length > kUploadChunkFloorBytes) {
      final chunked = await _uploadWithRufh(
        file: file,
        length: length,
        originalFileName: originalFileName,
        fields: fields,
        endpoint: savedEndpoint,
        cancelToken: cancelToken,
        onProgress: onProgress,
        logContext: logContext,
        httpClient: httpClient,
      );
      if (chunked != null) {
        return chunked;
      }
    }

    ProgressMultipartRequest buildRequest() {
      final request = ProgressMultipartRequest(
        'POST',
        Uri.parse('$savedEndpoint/assets'),
        abortTrigger: cancelToken?.future,
        onProgress: onProgress,
      );
      request.fields.addAll(fields);
      request.files.add(MultipartFile("assetData", file.openRead(), file.lengthSync(), filename: originalFileName));
      return request;
    }

    try {
      final client = httpClient ?? NetworkRepository.client;
      StreamedResponse response;
      try {
        response = await client.send(buildRequest());
      } on RequestAbortedException {
        rethrow;
      } on ClientException catch (error) {
        logger.warning("Upload $logContext failed before a response, resending once: $error");
        response = await client.send(buildRequest());
      }

      final responseBodyString = await response.stream.bytesToString();

      if (![200, 201].contains(response.statusCode)) {
        String? errorMessage;

        if (response.statusCode == 413) {
          errorMessage = 'Error(413) File is too large to upload';
          return UploadResult.error(statusCode: response.statusCode, errorMessage: errorMessage);
        }

        try {
          final error = jsonDecode(responseBodyString);
          errorMessage = error['message'] ?? error['error'];
        } catch (_) {
          errorMessage = responseBodyString.isNotEmpty
              ? responseBodyString
              : 'Upload failed with status ${response.statusCode}';
        }

        return UploadResult.error(statusCode: response.statusCode, errorMessage: errorMessage);
      }

      try {
        final responseBody = jsonDecode(responseBodyString);
        return UploadResult.success(remoteAssetId: responseBody['id'] as String);
      } catch (e) {
        return UploadResult.error(errorMessage: 'Failed to parse server response');
      }
    } on RequestAbortedException {
      logger.warning("Upload $logContext was cancelled");
      return UploadResult.cancelled();
    } catch (error, stackTrace) {
      logger.warning("Error uploading $logContext: $error: $stackTrace");
      return UploadResult.error(errorMessage: error.toString());
    }
  }

  Future<int?> maxAppendBytes({Client? httpClient}) async {
    final endpoint = Store.get(StoreKey.serverEndpoint);
    final client = httpClient ?? NetworkRepository.client;
    try {
      final response = await client.send(Request('OPTIONS', Uri.parse('$endpoint/upload')));
      final body = await response.stream.bytesToString();
      if (response.statusCode == 404 || response.statusCode == 405) {
        return null;
      }
      if (response.statusCode < 200 || response.statusCode >= 300) {
        logger.warning('Upload options failed: ${response.statusCode} $body');
        return null;
      }
      return _maxAppendSize(response.headers['upload-limit']);
    } catch (error) {
      logger.warning('Upload options request failed: $error');
      return null;
    }
  }

  int? _fileLength(File file) {
    try {
      return file.lengthSync();
    } catch (_) {
      return null;
    }
  }

  int? _maxAppendSize(String? header) {
    if (header == null) {
      return null;
    }
    final match = RegExp(r'(?:^|,\s*)max-append-size=(\d+)').firstMatch(header);
    final limit = int.tryParse(match?.group(1) ?? '');
    if (limit == null || limit <= 0) {
      return null;
    }
    return limit;
  }

  String _quoteStructured(String value) => '"${value.replaceAll('\\', '\\\\').replaceAll('"', '\\"')}"';

  Future<String> _checksumBase64(File file) async {
    final digest = await sha1.bind(file.openRead()).first;
    return base64Encode(digest.bytes);
  }

  String _assetDataHeader(Map<String, String> fields, String filename) {
    final created = fields['fileCreatedAt'] ?? DateTime.now().toUtc().toIso8601String();
    final modified = fields['fileModifiedAt'] ?? created;
    final parts = <String>[
      'device-asset-id=${_quoteStructured(fields['deviceAssetId'] ?? filename)}',
      'device-id=${_quoteStructured(fields['deviceId'] ?? 'mobile')}',
      'filename=${_quoteStructured(filename)}',
      'file-created-at=${_quoteStructured(created)}',
      'file-modified-at=${_quoteStructured(modified)}',
      'is-favorite=${fields['isFavorite'] == 'true' ? '?1' : '?0'}',
    ];
    final livePhotoVideoId = fields['livePhotoVideoId'];
    if (livePhotoVideoId != null && livePhotoVideoId.isNotEmpty) {
      parts.add('live-photo-video-id=${_quoteStructured(livePhotoVideoId)}');
    }
    final icloudId = fields['icloudId'] ?? _icloudIdFromMetadata(fields['metadata']);
    if (icloudId != null && icloudId.isNotEmpty) {
      parts.add('icloud-id=${_quoteStructured(icloudId)}');
    }
    return parts.join(', ');
  }

  String? _icloudIdFromMetadata(String? metadata) {
    if (metadata == null || metadata.isEmpty) {
      return null;
    }
    try {
      final decoded = jsonDecode(metadata);
      if (decoded is! List) {
        return null;
      }
      for (final item in decoded) {
        if (item is Map && item['value'] is Map) {
          final cloudId = (item['value'] as Map)['iCloudId'];
          if (cloudId is String && cloudId.isNotEmpty) {
            return cloudId;
          }
        }
      }
    } catch (_) {}
    return null;
  }

  /// Returns null when this server has no resumable upload limit below the file size.
  Future<UploadResult?> _uploadWithRufh({
    required File file,
    required int length,
    required String originalFileName,
    required Map<String, String> fields,
    required String endpoint,
    required Completer<void>? cancelToken,
    required void Function(int bytes, int totalBytes)? onProgress,
    required String logContext,
    required Client? httpClient,
  }) async {
    final client = httpClient ?? NetworkRepository.client;
    final limit = await maxAppendBytes(httpClient: client);
    if (limit == null || length <= limit) {
      return null;
    }

    var chunkSize = limit;
    var offset = 0;
    var url = Uri.parse('$endpoint/upload');
    var stalls = 0;
    final digest = await _checksumBase64(file);
    final assetData = _assetDataHeader(fields, originalFileName);

    while (offset < length) {
      if (cancelToken?.isCompleted == true) {
        return UploadResult.cancelled();
      }

      final sliceLength = chunkSize < length - offset ? chunkSize : length - offset;
      final complete = offset + sliceLength == length;
      final headers = <String, String>{
        'content-type': offset == 0 ? 'application/octet-stream' : 'application/partial-upload',
        'upload-draft-interop-version': '6',
        'upload-complete': complete ? '?1' : '?0',
        'upload-length': '$length',
      };
      if (offset == 0) {
        headers['repr-digest'] = 'sha=:$digest:';
        headers['x-immich-asset-data'] = assetData;
      } else {
        headers['upload-offset'] = '$offset';
      }

      final request = StreamedRequest(offset == 0 ? 'POST' : 'PATCH', url);
      request.headers.addAll(headers);
      request.contentLength = sliceLength;
      var sent = 0;
      final subscription = file.openRead(offset, offset + sliceLength).listen(
        (chunk) {
          sent += chunk.length;
          onProgress?.call(offset + sent, length);
          request.sink.add(chunk);
        },
        onError: request.sink.addError,
        onDone: request.sink.close,
        cancelOnError: true,
      );
      cancelToken?.future.then((_) {
        subscription.cancel();
        request.sink.close();
      });

      StreamedResponse response;
      try {
        response = await client.send(request);
      } on RequestAbortedException {
        return UploadResult.cancelled();
      } catch (error) {
        await subscription.cancel();
        stalls += 1;
        if (stalls > 5) {
          return UploadResult.error(errorMessage: 'Upload $logContext failed: $error');
        }
        continue;
      }

      final body = await response.stream.bytesToString();
      if (response.statusCode == 413) {
        final serverMax = _maxAppendSize(response.headers['upload-limit']);
        final halved = chunkSize ~/ 2 < kUploadChunkFloorBytes ? kUploadChunkFloorBytes : chunkSize ~/ 2;
        final next = serverMax != null && serverMax < chunkSize ? serverMax : halved;
        if (next >= sliceLength) {
          return UploadResult.error(statusCode: 413, errorMessage: 'Upload chunk exceeds the server limit');
        }
        chunkSize = next;
        continue;
      }

      if (response.statusCode == 409) {
        final serverOffset = int.tryParse(response.headers['upload-offset'] ?? '');
        if (serverOffset == null || serverOffset == offset) {
          stalls += 1;
          if (stalls > 5) {
            return UploadResult.error(statusCode: 409, errorMessage: body);
          }
          continue;
        }
        offset = serverOffset;
        stalls = 0;
        continue;
      }

      if (response.statusCode < 200 || response.statusCode >= 300) {
        return UploadResult.error(statusCode: response.statusCode, errorMessage: body.isEmpty ? 'Upload failed' : body);
      }

      stalls = 0;
      onProgress?.call(offset + sliceLength, length);
      if (complete || response.statusCode == 200) {
        try {
          final json = jsonDecode(body) as Map<String, dynamic>;
          return UploadResult.success(remoteAssetId: json['id'] as String);
        } catch (_) {
          return UploadResult.error(errorMessage: 'Failed to parse server response');
        }
      }

      final location = response.headers['location'];
      if (offset == 0 && location != null && location.isNotEmpty) {
        url = _resolveLocation(endpoint, location);
      }
      final serverOffset = int.tryParse(response.headers['upload-offset'] ?? '');
      offset = serverOffset ?? offset + sliceLength;
    }

    return UploadResult.error(errorMessage: 'Upload did not finish');
  }

  Uri _resolveLocation(String endpoint, String location) {
    if (location.startsWith('http')) {
      return Uri.parse(location);
    }
    if (location.startsWith('/')) {
      return Uri.parse(endpoint).replace(path: location);
    }
    return Uri.parse(endpoint).resolve(location);
  }
}

class ProgressMultipartRequest extends MultipartRequest with Abortable {
  ProgressMultipartRequest(super.method, super.url, {this.abortTrigger, this.onProgress});

  @override
  final Future<void>? abortTrigger;

  final void Function(int bytes, int totalBytes)? onProgress;

  @override
  ByteStream finalize() {
    final byteStream = super.finalize();
    if (onProgress == null) {
      return byteStream;
    }

    final total = contentLength;
    var bytes = 0;
    final stream = byteStream.transform(
      StreamTransformer.fromHandlers(
        handleData: (List<int> data, EventSink<List<int>> sink) {
          bytes += data.length;
          onProgress!(bytes, total);
          sink.add(data);
        },
      ),
    );
    return ByteStream(stream);
  }
}

class UploadResult {
  final bool isSuccess;
  final bool isCancelled;
  final String? remoteAssetId;
  final String? errorMessage;
  final int? statusCode;

  const UploadResult({
    required this.isSuccess,
    required this.isCancelled,
    this.remoteAssetId,
    this.errorMessage,
    this.statusCode,
  });

  factory UploadResult.success({required String remoteAssetId}) {
    return UploadResult(isSuccess: true, isCancelled: false, remoteAssetId: remoteAssetId);
  }

  factory UploadResult.error({String? errorMessage, int? statusCode}) {
    return UploadResult(isSuccess: false, isCancelled: false, errorMessage: errorMessage, statusCode: statusCode);
  }

  factory UploadResult.cancelled() {
    return const UploadResult(isSuccess: false, isCancelled: true);
  }
}
