use pallas::ledger::addresses::Address;
use serde::{Deserialize, Serialize};
use spec::cardano::{Block, Multiasset, Tx, TxCommon, TxOutput};
use utxorpc_spec::utxorpc::v1 as spec;

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

fn any_inputs_and_outputs<F>(tx: &TxCommon, filter: F) -> bool
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

    /// Conditions may match different parts of the same enclosing transaction.
    /// Boolean filters keep that scope; each leaf searches the parent and children.
    pub fn applies(&self, tx: &Tx) -> bool {
        if tx.common.is_none() && tx.sub_transactions.is_empty() {
            return false;
        }
        let mut common = tx.common.iter().chain(tx.sub_transactions.iter());
        match self {
            FilterConfig::All(criteria) => criteria.iter().all(|c| c.applies(tx)),
            FilterConfig::Any(criteria) => criteria.iter().any(|c| c.applies(tx)),
            FilterConfig::Address { address } => {
                common.any(|tx| any_inputs_and_outputs(tx, |out| address.eq(&out.address.to_vec())))
            }
            FilterConfig::Payment { payment } => common.any(|tx| {
                any_inputs_and_outputs(tx, |out| {
                    let out_addr = Address::from_bytes(&out.address);
                    match out_addr {
                        Ok(Address::Byron(b)) => b.to_vec().eq(payment),
                        Ok(Address::Shelley(s)) => s.payment().to_vec().eq(payment),
                        Ok(Address::Stake(_)) => false,
                        _ => true,
                    }
                })
            }),
            FilterConfig::Stake { stake } => common.any(|tx| {
                any_inputs_and_outputs(tx, |out| {
                    let out_addr = Address::from_bytes(&out.address);
                    match out_addr {
                        Ok(Address::Byron(_)) => false,
                        Ok(Address::Shelley(s)) => s.delegation().to_vec().to_vec().eq(stake),
                        Ok(Address::Stake(s)) => s.to_vec().eq(stake),
                        _ => true,
                    }
                })
            }),
            FilterConfig::Spent(token_filter) => {
                common.any(|tx| any_inputs_and_outputs(tx, |out| token_filter.applies(&out.assets)))
            }
            FilterConfig::Mint(token_filter) => common.any(|tx| token_filter.applies(&tx.mint)),
            FilterConfig::Withdraw { credential } => common.any(|tx| {
                tx.withdrawals
                    .iter()
                    .any(|w| w.reward_account == credential)
            }),
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
    use crate::utils::u64_to_bigint;

    fn transaction(common: TxCommon) -> Tx {
        Tx {
            common: Some(common),
            ..Default::default()
        }
    }

    #[test]
    fn block_filters_match_sub_transaction_outputs() {
        let address = vec![0x60; 29];
        let filter = FilterConfig::Address {
            address: address.clone(),
        };
        let matching = TxCommon {
            outputs: vec![TxOutput {
                address: address.into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut block = Block {
            body: Some(spec::cardano::BlockBody {
                tx: vec![transaction(TxCommon::default())],
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(!filter.applies_block(&block));
        block.body.as_mut().unwrap().tx[0]
            .sub_transactions
            .push(matching.clone());
        assert!(filter.applies_block(&block));
        block.body.as_mut().unwrap().tx[0].sub_transactions.clear();
        block.body.as_mut().unwrap().tx[0].common = Some(matching);
        assert!(filter.applies_block(&block));
        assert!(!filter.applies(&Tx::default()));
    }

    fn paying_to(address: &[u8]) -> TxCommon {
        TxCommon {
            outputs: vec![TxOutput {
                address: address.to_vec().into(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn minting(policy: &[u8]) -> TxCommon {
        TxCommon {
            mint: vec![Multiasset {
                policy_id: policy.to_vec().into(),
                assets: vec![],
            }],
            ..Default::default()
        }
    }

    #[test]
    fn all_combines_parent_and_sub_transaction_matches() {
        let address = vec![0x60; 29];
        let policy = vec![0x01; 28];
        let filter = FilterConfig::All(vec![
            Box::new(FilterConfig::Address {
                address: address.clone(),
            }),
            Box::new(FilterConfig::Mint(TokenFilter::Policy {
                policy: policy.clone(),
            })),
        ]);
        let mut tx = transaction(paying_to(&address));
        assert!(!filter.applies(&tx));
        tx.sub_transactions.push(minting(&policy));
        assert!(filter.applies(&tx));
    }

    #[test]
    fn all_combines_sibling_matches_but_not_unrelated_transactions() {
        let address = vec![0x60; 29];
        let policy = vec![0x01; 28];
        let filter = FilterConfig::All(vec![
            Box::new(FilterConfig::Address {
                address: address.clone(),
            }),
            Box::new(FilterConfig::Mint(TokenFilter::Policy {
                policy: policy.clone(),
            })),
        ]);
        let mut parent = transaction(TxCommon::default());
        parent.sub_transactions = vec![paying_to(&address), minting(&policy)];
        let mut block = Block {
            body: Some(spec::cardano::BlockBody {
                tx: vec![parent],
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(filter.applies_block(&block));

        // Moving the mint into an unrelated transaction must break the All match.
        let body = block.body.as_mut().unwrap();
        let mint = body.tx[0].sub_transactions.pop().unwrap();
        body.tx.push(transaction(mint));
        assert!(!filter.applies_block(&block));
    }

    #[test]
    fn nested_boolean_filters_share_the_enclosing_transaction_scope() {
        let address = vec![0x60; 29];
        let policy = vec![0x01; 28];
        let credential = vec![0x02; 29];
        let filter = FilterConfig::Any(vec![
            Box::new(FilterConfig::Address {
                address: vec![0xff; 29],
            }),
            Box::new(FilterConfig::All(vec![
                Box::new(FilterConfig::Address {
                    address: address.clone(),
                }),
                Box::new(FilterConfig::Any(vec![
                    Box::new(FilterConfig::Mint(TokenFilter::Policy {
                        policy: policy.clone(),
                    })),
                    Box::new(FilterConfig::Withdraw {
                        credential: credential.clone(),
                    }),
                ])),
            ])),
        ]);
        let mut tx = transaction(paying_to(&address));
        tx.sub_transactions = vec![TxCommon::default(), minting(&policy)];
        assert!(filter.applies(&tx));
        tx.sub_transactions[1] = TxCommon {
            withdrawals: vec![spec::cardano::Withdrawal {
                reward_account: credential.into(),
                coin: Some(u64_to_bigint(100)),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(filter.applies(&tx));
        tx.common = Some(TxCommon::default());
        assert!(!filter.applies(&tx));
        tx.common = Some(paying_to(&address));
        tx.sub_transactions.clear();
        assert!(!filter.applies(&tx));
    }

    #[test]
    fn output_filters_search_sub_transaction_outputs_and_resolved_inputs() {
        let payment = vec![0x11; 28];
        let stake = vec![0x22; 28];
        // Shelley base address with key payment/stake credentials on testnet.
        let address = [vec![0x00], payment.clone(), stake.clone()].concat();
        let policy = vec![0x33; 28];
        let asset_name = vec![0x44];
        let matching = TxOutput {
            address: address.clone().into(),
            assets: vec![Multiasset {
                policy_id: policy.clone().into(),
                assets: vec![spec::cardano::Asset {
                    name: asset_name.clone().into(),
                    quantity: Some(u64_to_bigint(1)),
                }],
            }],
            ..Default::default()
        };
        let filters = [
            FilterConfig::Address { address },
            FilterConfig::Payment { payment },
            FilterConfig::Stake { stake },
            FilterConfig::Spent(TokenFilter::Policy {
                policy: policy.clone(),
            }),
            FilterConfig::Spent(TokenFilter::AssetId { policy, asset_name }),
        ];
        for filter in filters {
            for as_input in [false, true] {
                let mut child = TxCommon::default();
                if as_input {
                    child.inputs.push(spec::cardano::TxInput {
                        as_output: Some(matching.clone()),
                        ..Default::default()
                    });
                } else {
                    child.outputs.push(matching.clone());
                }
                let mut tx = transaction(TxCommon::default());
                tx.sub_transactions = vec![TxCommon::default(), child];
                assert!(filter.applies(&tx), "{filter:?}, as_input={as_input}");

                // A missing parent common must not hide matching children.
                tx.common = None;
                assert!(filter.applies(&tx), "{filter:?}, as_input={as_input}");

                let child = &mut tx.sub_transactions[1];
                if as_input {
                    child.inputs[0].as_output = None;
                } else {
                    child.outputs[0] = TxOutput {
                        address: vec![0x00; 57].into(),
                        ..Default::default()
                    };
                }
                assert!(!filter.applies(&tx), "{filter:?}, as_input={as_input}");
            }
        }
    }

    #[test]
    fn mint_and_withdraw_filters_search_sub_transactions() {
        let policy = vec![0x01; 28];
        let credential = vec![0x02; 29];
        let cases = [
            (
                FilterConfig::Mint(TokenFilter::Policy {
                    policy: policy.clone(),
                }),
                minting(&policy),
            ),
            (
                FilterConfig::Withdraw {
                    credential: credential.clone(),
                },
                TxCommon {
                    withdrawals: vec![spec::cardano::Withdrawal {
                        reward_account: credential.into(),
                        coin: Some(u64_to_bigint(100)),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ),
        ];
        for (filter, matching) in cases {
            let mut tx = transaction(TxCommon::default());
            tx.sub_transactions = vec![TxCommon::default(), matching];
            for successful in [false, true] {
                // Filtering does not gate parent or child fields on success.
                tx.successful = successful;
                assert!(filter.applies(&tx), "{filter:?}, successful={successful}");
            }
            tx.common = None;
            assert!(filter.applies(&tx), "{filter:?}");
            tx.sub_transactions.pop();
            assert!(!filter.applies(&tx), "{filter:?}");
        }
    }

    #[test]
    fn empty_transactions_and_placeholder_filters_keep_existing_behavior() {
        let all = FilterConfig::All(vec![]);
        let any = FilterConfig::Any(vec![]);
        let signed = FilterConfig::Signed {
            credential: vec![0x01; 28],
        };
        let missing = Tx::default();
        assert!(!all.applies(&missing));
        assert!(!any.applies(&missing));
        assert!(!signed.applies(&missing));

        let empty = transaction(TxCommon::default());
        assert!(all.applies(&empty));
        assert!(!any.applies(&empty));
        assert!(signed.applies(&empty));
    }

    #[test]
    fn test_token_filter_policy_match() {
        let policy_id = vec![0x01, 0x02, 0x03];
        let filter = TokenFilter::Policy {
            policy: policy_id.clone(),
        };

        let assets = vec![Multiasset {
            policy_id: policy_id.clone().into(),
            assets: vec![],
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
            assets: vec![spec::cardano::Asset {
                name: asset_name.clone().into(),
                quantity: Some(u64_to_bigint(100)),
            }],
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
            assets: vec![spec::cardano::Asset {
                name: vec![0x04, 0x05].into(),
                quantity: Some(u64_to_bigint(100)),
            }],
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
            assets: vec![spec::cardano::Asset {
                name: vec![0xff, 0xee].into(),
                quantity: Some(u64_to_bigint(100)),
            }],
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

        let tx = transaction(TxCommon {
            mint: vec![
                Multiasset {
                    policy_id: policy1.into(),
                    assets: vec![],
                },
                Multiasset {
                    policy_id: policy2.into(),
                    assets: vec![],
                },
            ],
            ..Default::default()
        });

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
        let tx = transaction(TxCommon {
            mint: vec![Multiasset {
                policy_id: policy1.into(),
                assets: vec![],
            }],
            ..Default::default()
        });

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
        let tx = transaction(TxCommon {
            mint: vec![Multiasset {
                policy_id: policy1.into(),
                assets: vec![],
            }],
            ..Default::default()
        });

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
        let tx = transaction(TxCommon {
            mint: vec![Multiasset {
                policy_id: vec![0xff, 0xee, 0xdd].into(),
                assets: vec![],
            }],
            ..Default::default()
        });

        assert!(!filter.applies(&tx));
    }

    #[test]
    fn test_filter_config_mint() {
        let policy_id = vec![0xaa, 0xbb, 0xcc];
        let filter = FilterConfig::Mint(TokenFilter::Policy {
            policy: policy_id.clone(),
        });

        let tx = transaction(TxCommon {
            mint: vec![Multiasset {
                policy_id: policy_id.into(),
                assets: vec![],
            }],
            ..Default::default()
        });

        assert!(filter.applies(&tx));
    }

    #[test]
    fn test_filter_config_withdraw() {
        let credential = vec![0x11, 0x22, 0x33];
        let filter = FilterConfig::Withdraw {
            credential: credential.clone(),
        };

        let tx = transaction(TxCommon {
            withdrawals: vec![spec::cardano::Withdrawal {
                reward_account: credential.into(),
                coin: Some(u64_to_bigint(1000)),
                redeemer: None,
            }],
            ..Default::default()
        });

        assert!(filter.applies(&tx));
    }

    #[test]
    fn test_filter_config_withdraw_no_match() {
        let filter = FilterConfig::Withdraw {
            credential: vec![0x11, 0x22, 0x33],
        };

        let tx = transaction(TxCommon {
            withdrawals: vec![spec::cardano::Withdrawal {
                reward_account: vec![0xff, 0xee, 0xdd].into(),
                coin: Some(u64_to_bigint(1000)),
                redeemer: None,
            }],
            ..Default::default()
        });

        assert!(!filter.applies(&tx));
    }
}
