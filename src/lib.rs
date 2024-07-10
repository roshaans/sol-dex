#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(non_snake_case)]

mod dapps;
mod pb;
mod utils;

use pb::sf::solana::dex::trades::v1::{Output, TradeData};
use substreams::log;
use substreams_solana::pb::sf::solana::r#type::v1::InnerInstructions;
use substreams_solana::pb::sf::solana::r#type::v1::{Block, TokenBalance};
use utils::convert_to_date;
use utils::get_mint;
mod trade_instruction;

#[substreams::handlers::map]
fn map_block(block: Block) -> Result<Output, substreams::errors::Error> {
    process_block(block)
}

fn process_block(block: Block) -> Result<Output, substreams::errors::Error> {
    let slot = block.slot;
    let parent_slot = block.parent_slot;
    let timestamp = block.block_time.as_ref();
    let mut data: Vec<TradeData> = vec![];
    if timestamp.is_some() {
        let timestamp = timestamp.unwrap().timestamp;
        for trx in block.transactions_owned() {
            let accounts = trx.resolved_accounts_as_strings();
            if let Some(transaction) = trx.transaction {
                let meta = trx.meta.unwrap();
                let pre_balances = meta.pre_balances;
                let post_balances = meta.post_balances;
                let pre_token_balances = meta.pre_token_balances;
                let post_token_balances = meta.post_token_balances;

                let msg = transaction.message.unwrap();

                for (idx, inst) in msg.instructions.into_iter().enumerate() {
                    let inner_instructions: Vec<InnerInstructions> =
                        filter_inner_instructions(&meta.inner_instructions, idx as u32);

                    let program = &accounts[inst.program_id_index as usize];
                    let trade_data = get_trade_instruction(
                        program,
                        inst.data,
                        &inst.accounts,
                        &accounts,
                        &pre_token_balances,
                        &post_token_balances,
                        &"".to_string(),
                        false,
                        &inner_instructions,
                    );
                    if trade_data.is_some() {
                        let td = trade_data.unwrap();

                        data.push(TradeData {
                            block_date: convert_to_date(timestamp),
                            tx_id: bs58::encode(&transaction.signatures[0]).into_string(),
                            block_slot: slot,
                            block_time: timestamp,
                            signer: accounts.get(0).unwrap().to_string(),
                            pool_address: td.amm,
                            base_mint: get_mint(&td.vault_a, &post_token_balances, &accounts),
                            quote_mint: get_mint(&td.vault_b, &pre_token_balances, &accounts),
                            base_amount: get_amt(
                                &td.vault_a,
                                &pre_token_balances,
                                &post_token_balances,
                                &accounts,
                            ),
                            quote_amount: get_amt(
                                &td.vault_b,
                                &pre_token_balances,
                                &post_token_balances,
                                &accounts,
                            ),
                            base_vault: td.vault_a,
                            quote_vault: td.vault_b,
                            is_inner_instruction: false,
                            instruction_index: idx as u32,
                            instruction_type: td.name,
                            inner_instruxtion_index: 0,
                            outer_program: td.dapp_address,
                            inner_program: "".to_string(),
                            txn_fee: meta.fee,
                            signer_sol_change: get_signer_balance_change(
                                &pre_balances,
                                &post_balances,
                            ),
                        });
                    }

                    meta.inner_instructions
                        .iter()
                        .filter(|inner_instruction| inner_instruction.index == idx as u32)
                        .for_each(|inner_instruction| {
                            inner_instruction.instructions.iter().enumerate().for_each(
                                |(inner_idx, inner_inst)| {
                                    let inner_program =
                                        &accounts[inner_inst.program_id_index as usize];
                                    let trade_data = get_trade_instruction(
                                        inner_program,
                                        inner_inst.data.clone(),
                                        &inner_inst.accounts,
                                        &accounts,
                                        &pre_token_balances,
                                        &post_token_balances,
                                        &program.to_string(),
                                        true,
                                        &inner_instructions,
                                    );

                                    if trade_data.is_some() {
                                        let td = trade_data.unwrap();

                                        data.push(TradeData {
                                            block_date: convert_to_date(timestamp),
                                            tx_id: bs58::encode(&transaction.signatures[0])
                                                .into_string(),
                                            block_slot: slot,
                                            block_time: timestamp,
                                            signer: accounts.get(0).unwrap().to_string(),
                                            pool_address: td.amm,
                                            base_mint: get_mint(
                                                &td.vault_a,
                                                &pre_token_balances,
                                                &accounts,
                                            ),
                                            quote_mint: get_mint(
                                                &td.vault_b,
                                                &pre_token_balances,
                                                &accounts,
                                            ),
                                            base_amount: get_amt(
                                                &td.vault_a,
                                                &pre_token_balances,
                                                &post_token_balances,
                                                &accounts,
                                            ),
                                            quote_amount: get_amt(
                                                &td.vault_b,
                                                &pre_token_balances,
                                                &post_token_balances,
                                                &accounts,
                                            ),
                                            base_vault: td.vault_a,
                                            quote_vault: td.vault_b,
                                            is_inner_instruction: true,
                                            instruction_index: idx as u32,
                                            instruction_type: td.name,
                                            inner_instruxtion_index: inner_idx as u32,
                                            outer_program: program.to_string(),
                                            inner_program: td.dapp_address,
                                            txn_fee: meta.fee,
                                            signer_sol_change: get_signer_balance_change(
                                                &pre_balances,
                                                &post_balances,
                                            ),
                                        });
                                    }
                                },
                            )
                        });
                }
            }
        }
    }

    log::info!("{:#?}", slot);
    Ok(Output { data })
}

fn get_trade_instruction(
    dapp_address: &String,
    instruction_data: Vec<u8>,
    account_indices: &Vec<u8>,
    accounts: &Vec<String>,
    pre_token_balances: &Vec<TokenBalance>,
    post_token_balances: &Vec<TokenBalance>,
    outer_program: &String,
    is_inner: bool,
    inner_instructions: &Vec<InnerInstructions>,
) -> Option<trade_instruction::TradeInstruction> {
    let input_accounts = prepare_input_accounts(account_indices, accounts);

    let mut result = None;
    match dapp_address.as_str() {
        "CLMM9tUoggJu2wagPkkqs9eFG4BWhVBZWkP1qv3Sp7tR" => {
            result =
                dapps::dapp_CLMM9tUoggJu2wagPkkqs9eFG4BWhVBZWkP1qv3Sp7tR::parse_trade_instruction(
                    instruction_data,
                    input_accounts,
                );
        }
        "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB" => {
            result =
                dapps::dapp_Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB::parse_trade_instruction(
                    instruction_data,
                    input_accounts,
                );
        }
        "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc" => {
            result =
                dapps::dapp_whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc::parse_trade_instruction(
                    instruction_data,
                    input_accounts,
                );
        }
        "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK" => {
            result =
                dapps::dapp_CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK::parse_trade_instruction(
                    instruction_data,
                    input_accounts,
                );
        }
        "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8" => {
            result =
                dapps::dapp_675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8::parse_trade_instruction(
                    instruction_data,
                    input_accounts,
                    &post_token_balances,
                    accounts,
                );
        }
        "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo" => {
            result =
                dapps::dapp_LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo::parse_trade_instruction(
                    instruction_data,
                    input_accounts,
                );
        }
        "9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP" => {
            result =
                dapps::dapp_9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP::parse_trade_instruction(
                    instruction_data,
                    input_accounts,
                );
        }

        _ => {}
    }

    return result;
}

fn prepare_input_accounts(account_indices: &Vec<u8>, accounts: &Vec<String>) -> Vec<String> {
    let mut instruction_accounts: Vec<String> = vec![];
    for (index, &el) in account_indices.iter().enumerate() {
        instruction_accounts.push(accounts.as_slice()[el as usize].to_string());
    }
    return instruction_accounts;
}

fn get_amt(
    address: &String,
    pre_token_balances: &Vec<TokenBalance>,
    post_token_balances: &Vec<TokenBalance>,
    accounts: &Vec<String>,
) -> f64 {
    let index = accounts.iter().position(|r| r == address).unwrap();

    let mut pre_balance: f64 = 0 as f64;
    let mut post_balance: f64 = 0 as f64;

    pre_token_balances
        .iter()
        .filter(|token_balance| token_balance.account_index == index as u32)
        .for_each(|token_balance: &TokenBalance| {
            pre_balance = token_balance.ui_token_amount.clone().unwrap().ui_amount;
        });

    post_token_balances
        .iter()
        .filter(|token_balance| token_balance.account_index == index as u32)
        .for_each(|token_balance: &TokenBalance| {
            post_balance = token_balance.ui_token_amount.clone().unwrap().ui_amount;
        });

    return post_balance - pre_balance;
}

fn get_signer_balance_change(pre_balances: &Vec<u64>, post_balances: &Vec<u64>) -> i64 {
    return (post_balances[0] - pre_balances[0]) as i64;
}

fn filter_inner_instructions(
    meta_inner_instructions: &Vec<InnerInstructions>,
    idx: u32,
) -> Vec<InnerInstructions> {
    let mut inner_instructions: Vec<InnerInstructions> = vec![];
    let mut iterator = meta_inner_instructions.iter();
    while let Some(inner_inst) = iterator.next() {
        if inner_inst.index == idx as u32 {
            inner_instructions.push(inner_inst.clone());
        }
    }
    return inner_instructions;
}
