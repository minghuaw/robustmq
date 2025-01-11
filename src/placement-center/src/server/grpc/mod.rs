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

macro_rules! handle_raft_client_write {
    ($self:ident, $data:ident, $reply_ty:tt) => {
        match $self.raft_machine_apply.client_write($data).await {
            Ok(_) => Ok(Response::new(<$reply_ty>::default())),
            Err(e) => {
                match protocol::placement_center::openraft_shared::ForwardToLeader::try_from(e) {
                    Ok(forward_to_leader) => {
                        let reply = $reply_ty {
                            forward_to_leader: Some(forward_to_leader),
                        };
                        Ok(Response::new(reply))
                    }
                    Err(e) => Err(Status::cancelled(e.to_string())),
                }
            }
        }
    };
}
use handle_raft_client_write;

pub mod service_inner;
pub mod service_journal;
pub mod service_kv;
pub mod service_mqtt;
pub mod services_openraft;
pub mod validate;
