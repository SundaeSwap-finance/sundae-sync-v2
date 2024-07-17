use anyhow::*;
use pallas::ledger::addresses::Address;
use utxorpc::spec::cardano::{Multiasset, Tx, TxOutput};

pub enum TokenFilter {
    Policy(Vec<u8>),
    AssetId(Vec<u8>, Vec<u8>),
}

pub enum FilterConfig {
    All(Vec<Box<FilterConfig>>),
    Any(Vec<Box<FilterConfig>>),
    Address(Vec<u8>),
    Payment(Vec<u8>),
    Stake(Vec<u8>),
    Spent(TokenFilter),
    Mint(TokenFilter),
    Withdraw(Vec<u8>),
    Signed(Vec<u8>),
}

fn any_inputs_and_outputs<F>(tx: &Tx, f: F) -> Result<bool>
where
    F: Fn(&TxOutput) -> Result<bool>,
{
    for input in &tx.inputs {
        if let Some(output) = &input.as_output {
            if f(&output)? {
                return Ok(true);
            }
        } else {
            // We don't know whether this input applied, so we default to true
            // because it's safer to announce extra transacitons than to miss one
            return Ok(true);
        }
    }
    for output in &tx.outputs {
        if f(output)? {
            return Ok(true);
        }
    }
    return Ok(false);
}

impl TokenFilter {
    pub fn applies(&self, assets: &Vec<Multiasset>) -> bool {
        match self {
            TokenFilter::Policy(policy) => assets.iter().any(|a| a.policy_id == policy),
            TokenFilter::AssetId(policy, asset_name) => assets
                .iter()
                .any(|a| a.policy_id == policy && a.assets.iter().any(|a| a.name == asset_name)),
        }
    }
}

impl FilterConfig {
    pub fn applies(&self, tx: &Tx) -> Result<bool> {
        match self {
            FilterConfig::All(criteria) => {
                Ok(criteria.iter().all(|c| c.applies(tx).unwrap_or(false)))
            }
            FilterConfig::Any(criteria) => {
                Ok(criteria.iter().any(|c| c.applies(tx).unwrap_or(false)))
            }
            FilterConfig::Address(addr) => {
                any_inputs_and_outputs(tx, |out| Ok(addr.eq(&out.address.to_vec())))
            }
            FilterConfig::Payment(payment) => any_inputs_and_outputs(tx, |out| {
                let out_addr = Address::from_bytes(&out.address)?;
                Ok(match out_addr {
                    Address::Byron(b) => b.to_vec().eq(payment),
                    Address::Shelley(s) => s.payment().to_vec().eq(payment),
                    Address::Stake(_) => false,
                })
            }),
            FilterConfig::Stake(stake) => any_inputs_and_outputs(tx, |out| {
                let out_addr = Address::from_bytes(&out.address)?;
                Ok(match out_addr {
                    Address::Byron(_) => false,
                    Address::Shelley(s) => s.delegation().to_vec().to_vec().eq(stake),
                    Address::Stake(s) => s.to_vec().eq(stake),
                })
            }),
            FilterConfig::Spent(token_filter) => {
                any_inputs_and_outputs(tx, |out| Ok(token_filter.applies(&out.assets)))
            }
            FilterConfig::Mint(token_filter) => Ok(token_filter.applies(&tx.mint)),
            FilterConfig::Withdraw(rewards) => {
                Ok(tx.withdrawals.iter().any(|w| w.reward_account == rewards))
            }
            FilterConfig::Signed(_pkh) => {
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
                Ok(true)
            }
        }
    }
}
