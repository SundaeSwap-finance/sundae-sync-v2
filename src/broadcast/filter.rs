use pallas::ledger::addresses::Address;
use serde::{Deserialize, Serialize};
use utxorpc::spec::cardano::{Block, Multiasset, Tx, TxOutput};

#[derive(Serialize, Deserialize, Debug)]
pub enum TokenFilter {
    Policy {
        #[serde(with = "serde_bytes")]
        policy: Vec<u8>,
    },
    AssetId {
        #[serde(with = "serde_bytes")]
        policy: Vec<u8>,
        #[serde(with = "serde_bytes")]
        asset_name: Vec<u8>,
    },
}

#[derive(Serialize, Deserialize, Debug)]
pub enum FilterConfig {
    All(Vec<Box<FilterConfig>>),
    Any(Vec<Box<FilterConfig>>),
    Address {
        #[serde(with = "serde_bytes")]
        address: Vec<u8>,
    },
    Payment {
        #[serde(with = "serde_bytes")]
        payment: Vec<u8>,
    },
    Stake {
        #[serde(with = "serde_bytes")]
        stake: Vec<u8>,
    },
    Spent(TokenFilter),
    Mint(TokenFilter),
    Withdraw {
        #[serde(with = "serde_bytes")]
        credential: Vec<u8>,
    },
    Signed {
        #[serde(with = "serde_bytes")]
        credential: Vec<u8>,
    },
}

fn any_inputs_and_outputs<F>(tx: &Tx, filter: F) -> bool
where
    F: Fn(&TxOutput) -> bool,
{
    let (inputs, outputs) = (tx.inputs.iter(), tx.outputs.iter());
    if outputs.into_iter().any(&filter) {
        return true;
    }
    inputs
        .into_iter()
        // TODO: find out why as_output is coming back none so often
        .any(|input| input.as_output.as_ref().is_some_and(&filter))
}

impl TokenFilter {
    pub fn applies(&self, assets: &[Multiasset]) -> bool {
        match self {
            TokenFilter::Policy { policy } => assets.iter().any(|a| a.policy_id == policy),
            TokenFilter::AssetId { policy, asset_name } => assets
                .iter()
                .any(|a| a.policy_id == policy && a.assets.iter().any(|a| a.name == asset_name)),
        }
    }
}

impl FilterConfig {
    pub fn applies_block(&self, b: &Block) -> bool {
        b.body
            .as_ref()
            .is_none_or(|body| body.tx.iter().any(|tx| self.applies(tx)))
    }

    pub fn applies(&self, tx: &Tx) -> bool {
        match self {
            FilterConfig::All(criteria) => criteria.iter().all(|c| c.applies(tx)),
            FilterConfig::Any(criteria) => criteria.iter().any(|c| c.applies(tx)),
            FilterConfig::Address { address } => {
                any_inputs_and_outputs(tx, |out| address.eq(&out.address.to_vec()))
            }
            FilterConfig::Payment { payment } => any_inputs_and_outputs(tx, |out| {
                let out_addr = Address::from_bytes(&out.address);
                match out_addr {
                    Ok(Address::Byron(b)) => b.to_vec().eq(payment),
                    Ok(Address::Shelley(s)) => s.payment().to_vec().eq(payment),
                    Ok(Address::Stake(_)) => false,
                    _ => true,
                }
            }),
            FilterConfig::Stake { stake } => any_inputs_and_outputs(tx, |out| {
                let out_addr = Address::from_bytes(&out.address);
                match out_addr {
                    Ok(Address::Byron(_)) => false,
                    Ok(Address::Shelley(s)) => s.delegation().to_vec().to_vec().eq(stake),
                    Ok(Address::Stake(s)) => s.to_vec().eq(stake),
                    _ => true,
                }
            }),
            FilterConfig::Spent(token_filter) => {
                any_inputs_and_outputs(tx, |out| token_filter.applies(&out.assets))
            }
            FilterConfig::Mint(token_filter) => token_filter.applies(&tx.mint),
            FilterConfig::Withdraw { credential } => tx
                .withdrawals
                .iter()
                .any(|w| w.reward_account == credential),
            FilterConfig::Signed { .. } => {
                /*
                if let Some(witnesses) = tx.witnesses {
                    Ok(witnesses.vkeywitness.iter().any(|vk| {
                        let mut hasher = Hasher::<224>::new();
                        hasher.input(&vk.vkey);
                        pkh == &hasher.finalize().to_vec()
                    }))
                } else {
                    Ok(true)
                }
                */
                true
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_filter_policy_match() {
        let policy_id = vec![0x01, 0x02, 0x03];
        let filter = TokenFilter::Policy {
            policy: policy_id.clone(),
        };

        let assets = vec![Multiasset {
            policy_id: policy_id.clone().into(),
            assets: vec![],
            redeemer: None,
        }];

        assert!(filter.applies(&assets));
    }

    #[test]
    fn test_token_filter_policy_no_match() {
        let filter = TokenFilter::Policy {
            policy: vec![0x01, 0x02, 0x03],
        };

        let assets = vec![Multiasset {
            policy_id: vec![0xff, 0xee, 0xdd].into(),
            assets: vec![],
            redeemer: None,
        }];

        assert!(!filter.applies(&assets));
    }

    #[test]
    fn test_token_filter_asset_id_match() {
        let policy_id = vec![0x01, 0x02, 0x03];
        let asset_name = vec![0x04, 0x05];
        let filter = TokenFilter::AssetId {
            policy: policy_id.clone(),
            asset_name: asset_name.clone(),
        };

        let assets = vec![Multiasset {
            policy_id: policy_id.clone().into(),
            assets: vec![utxorpc::spec::cardano::Asset {
                name: asset_name.clone().into(),
                output_coin: 100,
                mint_coin: 0,
            }],
            redeemer: None,
        }];

        assert!(filter.applies(&assets));
    }

    #[test]
    fn test_token_filter_asset_id_wrong_policy() {
        let filter = TokenFilter::AssetId {
            policy: vec![0x01, 0x02, 0x03],
            asset_name: vec![0x04, 0x05],
        };

        let assets = vec![Multiasset {
            policy_id: vec![0xff, 0xee, 0xdd].into(),
            assets: vec![utxorpc::spec::cardano::Asset {
                name: vec![0x04, 0x05].into(),
                output_coin: 100,
                mint_coin: 0,
            }],
            redeemer: None,
        }];

        assert!(!filter.applies(&assets));
    }

    #[test]
    fn test_token_filter_asset_id_wrong_name() {
        let policy_id = vec![0x01, 0x02, 0x03];
        let filter = TokenFilter::AssetId {
            policy: policy_id.clone(),
            asset_name: vec![0x04, 0x05],
        };

        let assets = vec![Multiasset {
            policy_id: policy_id.clone().into(),
            assets: vec![utxorpc::spec::cardano::Asset {
                name: vec![0xff, 0xee].into(),
                output_coin: 100,
                mint_coin: 0,
            }],
            redeemer: None,
        }];

        assert!(!filter.applies(&assets));
    }

    #[test]
    fn test_token_filter_empty_assets() {
        let filter = TokenFilter::Policy {
            policy: vec![0x01, 0x02, 0x03],
        };
        assert!(!filter.applies(&[]));
    }

    #[test]
    fn test_filter_config_all_match() {
        let policy1 = vec![0x01, 0x02, 0x03];
        let policy2 = vec![0x04, 0x05, 0x06];

        let filter = FilterConfig::All(vec![
            Box::new(FilterConfig::Mint(TokenFilter::Policy {
                policy: policy1.clone(),
            })),
            Box::new(FilterConfig::Mint(TokenFilter::Policy {
                policy: policy2.clone(),
            })),
        ]);

        let tx = Tx {
            mint: vec![
                Multiasset {
                    policy_id: policy1.into(),
                    assets: vec![],
                    redeemer: None,
                },
                Multiasset {
                    policy_id: policy2.into(),
                    assets: vec![],
                    redeemer: None,
                },
            ],
            ..Default::default()
        };

        assert!(filter.applies(&tx));
    }

    #[test]
    fn test_filter_config_all_no_match() {
        let policy1 = vec![0x01, 0x02, 0x03];
        let policy2 = vec![0x04, 0x05, 0x06];

        let filter = FilterConfig::All(vec![
            Box::new(FilterConfig::Mint(TokenFilter::Policy {
                policy: policy1.clone(),
            })),
            Box::new(FilterConfig::Mint(TokenFilter::Policy {
                policy: policy2.clone(),
            })),
        ]);

        // Only has policy1, missing policy2
        let tx = Tx {
            mint: vec![Multiasset {
                policy_id: policy1.into(),
                assets: vec![],
                redeemer: None,
            }],
            ..Default::default()
        };

        assert!(!filter.applies(&tx));
    }

    #[test]
    fn test_filter_config_any_match() {
        let policy1 = vec![0x01, 0x02, 0x03];
        let policy2 = vec![0x04, 0x05, 0x06];

        let filter = FilterConfig::Any(vec![
            Box::new(FilterConfig::Mint(TokenFilter::Policy {
                policy: policy1.clone(),
            })),
            Box::new(FilterConfig::Mint(TokenFilter::Policy { policy: policy2 })),
        ]);

        // Only has policy1, but that's enough for Any
        let tx = Tx {
            mint: vec![Multiasset {
                policy_id: policy1.into(),
                assets: vec![],
                redeemer: None,
            }],
            ..Default::default()
        };

        assert!(filter.applies(&tx));
    }

    #[test]
    fn test_filter_config_any_no_match() {
        let filter = FilterConfig::Any(vec![
            Box::new(FilterConfig::Mint(TokenFilter::Policy {
                policy: vec![0x01, 0x02, 0x03],
            })),
            Box::new(FilterConfig::Mint(TokenFilter::Policy {
                policy: vec![0x04, 0x05, 0x06],
            })),
        ]);

        // Has neither policy
        let tx = Tx {
            mint: vec![Multiasset {
                policy_id: vec![0xff, 0xee, 0xdd].into(),
                assets: vec![],
                redeemer: None,
            }],
            ..Default::default()
        };

        assert!(!filter.applies(&tx));
    }

    #[test]
    fn test_filter_config_mint() {
        let policy_id = vec![0xaa, 0xbb, 0xcc];
        let filter = FilterConfig::Mint(TokenFilter::Policy {
            policy: policy_id.clone(),
        });

        let tx = Tx {
            mint: vec![Multiasset {
                policy_id: policy_id.into(),
                assets: vec![],
                redeemer: None,
            }],
            ..Default::default()
        };

        assert!(filter.applies(&tx));
    }

    #[test]
    fn test_filter_config_withdraw() {
        let credential = vec![0x11, 0x22, 0x33];
        let filter = FilterConfig::Withdraw {
            credential: credential.clone(),
        };

        let tx = Tx {
            withdrawals: vec![utxorpc::spec::cardano::Withdrawal {
                reward_account: credential.into(),
                coin: 1000,
                redeemer: None,
            }],
            ..Default::default()
        };

        assert!(filter.applies(&tx));
    }

    #[test]
    fn test_filter_config_withdraw_no_match() {
        let filter = FilterConfig::Withdraw {
            credential: vec![0x11, 0x22, 0x33],
        };

        let tx = Tx {
            withdrawals: vec![utxorpc::spec::cardano::Withdrawal {
                reward_account: vec![0xff, 0xee, 0xdd].into(),
                coin: 1000,
                redeemer: None,
            }],
            ..Default::default()
        };

        assert!(!filter.applies(&tx));
    }
}
