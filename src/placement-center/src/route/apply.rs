// Copyright 2023 RobustMQ Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::time::Duration;

use openraft::error::{ClientWriteError, RaftError};
use openraft::raft::ClientWriteResponse;
use openraft::Raft;
use protocol::placement_center::openraft_shared::ForwardToLeader;
use tokio::time::timeout;

use crate::core::error::PlacementCenterError;
use crate::raft::typeconfig::TypeConfig;
use crate::route::data::StorageData;

pub struct RaftMachineApply {
    pub openraft_node: Raft<TypeConfig>,
}

// pub enum RaftWriteResult {
//     Ok(ClientWriteResponse<TypeConfig>),
//     Err(ForwardToLeader),
// }

impl RaftMachineApply {
    pub fn new(openraft_node: Raft<TypeConfig>) -> Self {
        RaftMachineApply { openraft_node }
    }

    #[must_use]
    pub async fn client_write(
        &self,
        data: StorageData,
    ) -> Result<Option<ClientWriteResponse<TypeConfig>>, PlacementCenterError> {
        self.raft_write(data).await.map(Some)
    }

    #[inline]
    #[must_use]
    async fn raft_write(
        &self,
        data: StorageData,
    ) -> Result<ClientWriteResponse<TypeConfig>, PlacementCenterError> {
        timeout(
            Duration::from_secs(10),
            self.openraft_node.client_write(data),
        )
        .await?
        .map_err(Into::into)
    }
}
