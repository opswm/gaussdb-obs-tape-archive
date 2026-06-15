#!/usr/bin/python
# -*- coding:utf-8 -*-
# Copyright 2019 Huawei Technologies Co.,Ltd.
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use
# this file except in compliance with the License.  You may obtain a copy of the
# License at

# http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations under the License.


from obs.ilog import LogConf
from obs.client import ObsClient
from obs.model import CompletePart, Permission, StorageClass, EventType, RestoreTier, Group, Grantee, Grant
from obs.model import ExtensionGrant, Owner, ACL, Condition, DateTime, SseCHeader, SseKmsHeader, CopyObjectHeader
from obs.model import SetObjectMetadataHeader,RenameFileHeader, CorsRule, CreateBucketHeader, ErrorDocument, IndexDocument, Expiration
from obs.model import NoncurrentVersionExpiration, GetObjectHeader, HeadPermission, Lifecycle, Notification
from obs.model import TopicConfiguration, FunctionGraphConfiguration, FilterRule, Replication, ReplicationRule
from obs.model import Options, PutObjectHeader, AppendObjectHeader, AppendObjectContent, RedirectAllRequestTo
from obs.model import Redirect, RoutingRule, Tag, TagInfo, Transition, NoncurrentVersionTransition, Rule, Versions, AbortIncompleteMultipartUpload
from obs.model import Object, WebsiteConfiguration, Logging, CompleteMultipartUploadRequest, DeleteObjectsRequest,CustomDomainConfiguration
from obs.model import ListMultipartUploadsRequest, GetObjectRequest, UploadFileHeader, Payer, ClientVerify
from obs.model import ExtensionHeader, FetchStatus, BucketAliasModel, ListBucketAliasModel
from obs.workflow import WorkflowClient

# 加密模块为可选依赖 — 基础 OBS 操作 (list/get/put/delete) 不需要
try:
    from obs.crypto_client import CryptoObsClient
except ImportError:
    CryptoObsClient = None

try:
    from obs.obs_cipher_suite import CTRCipherGenerator
    from obs.obs_cipher_suite import CtrRSACipherGenerator
except ImportError:
    CTRCipherGenerator = None
    CtrRSACipherGenerator = None

__all__ = [
    'LogConf',
    'ObsClient',
    'CompletePart',
    'Permission',
    'StorageClass',
    'EventType',
    'RestoreTier',
    'Group',
    'Grantee',
    'Grant',
    'ExtensionGrant',
    'Owner',
    'ACL',
    'Condition',
    'DateTime',
    'SseCHeader',
    'SseKmsHeader',
    'CopyObjectHeader',
    'SetObjectMetadataHeader',
    'RenameFileHeader',
    'CorsRule',
    'CreateBucketHeader',
    'ErrorDocument',
    'IndexDocument',
    'Expiration',
    'NoncurrentVersionExpiration',
    'AbortIncompleteMultipartUpload',
    'GetObjectHeader',
    'HeadPermission',
    'Lifecycle',
    'Notification',
    'TopicConfiguration',
    'FunctionGraphConfiguration',
    'FilterRule',
    'Replication',
    'ReplicationRule',
    'Options',
    'PutObjectHeader',
    'AppendObjectHeader',
    'AppendObjectContent',
    'RedirectAllRequestTo',
    'Redirect',
    'RoutingRule',
    'Tag',
    'TagInfo',
    'Transition',
    'NoncurrentVersionTransition',
    'Rule',
    'Versions',
    'Object',
    'WebsiteConfiguration',
    'Logging',
    'CompleteMultipartUploadRequest',
    'DeleteObjectsRequest',
    'ListMultipartUploadsRequest',
    'GetObjectRequest',
    'UploadFileHeader',
    'Payer',
    'ExtensionHeader',
    'FetchStatus',
    'WorkflowClient',
    'CryptoObsClient',
    'CTRCipherGenerator',
    'CtrRSACipherGenerator',
    'BucketAliasModel',
    'ListBucketAliasModel',
    'CustomDomainConfiguration',
    'ClientVerify'
]
