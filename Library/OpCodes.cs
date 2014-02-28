/*
 * SHS -- The Scalable Hyperlink Store 
 * 
 * Copyright (c) Microsoft Corporation
 * All rights reserved. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0 
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */

namespace SHS {
  internal enum OpCodes : uint {
    // Server to leader
    AdvertiseService    = 0xf1000001,
    ReplicaServers      = 0xf1000002,

    // Leader to server
    AddRowOnSecondary   = 0xf1000003,
    AddChkptOnSecondary = 0xf1000004,

    // Server to server
    PullFile            = 0xf1000005,
    MapOldToNewUids     = 0xf1000006,

    // Client to leader
    NumAvailableServers = 0xf1000007, 
    ListStores          = 0xf1000008,
    CreateStore         = 0xf1000009,
    StatStore           = 0xf1000010, 
    OpenStore           = 0xf1000011,
    SealStore           = 0xf1000012,
    DeleteStore         = 0xf1000013,

    // Client to server
    CreatePartition     = 0xf1000014,  
    OpenPartition       = 0xf1000015,  
    AddPageLinks              = 0xf1000016,
    Request             = 0xf1000017,
    Relinquish          = 0xf1000018,
    NumUrls             = 0xf1000019,
    NumLinks            = 0xf1000020,
    MaxDegree           = 0xf1000021,
    UrlToUid            = 0xf1000022,
    UidToUrl            = 0xf1000023,
    BatchedUrlToUid     = 0xf1000024,
    BatchedUidToUrl     = 0xf1000025,
    SampleLinks         = 0xf1000026,
    BatchedSampleLinks  = 0xf1000027,
    GetDegree           = 0xf1000028,
    BatchedGetDegree    = 0xf1000029,
    AllocateUidState    = 0xf1000030,
    FreeUidState        = 0xf1000031,
    SetUidState         = 0xf1000032,
    GetUidState         = 0xf1000033,
    BatchedSetUidState  = 0xf1000034,
    BatchedGetUidState  = 0xf1000035,
    CheckpointUidStates = 0xf1000036,
  }
}
