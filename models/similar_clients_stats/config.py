import os

# Названия таблиц
CLIENTS_PORTFOLIO_TABLE = os.getenv('CLIENTS_PORTFOLIO_TABLE') or None

SUM_AGGR_COLS = ['active', 'client_of_tochka', 'block_ufm', 'tax_restrictions', 'sp_big_plans', 'sp_fast_growth',
                'sp_all_world', 'sp_open_opportunities', 'sp_first_step', 'sp_own_business', 'sp_business_package',
                'corp_card', 'factoring', 'nkl', 'nso', 'electron_guar', 'business_prepaid', 'overdraft', 'deposit',
                'deposit_gt_5m', 'deposit_gt_3m', 'vkl', 'dvs', 'letters_of_credit', 'credit', 'rko', 'rko_cur',
                'warranty', 'bss', 'inkass', 'provision_get_pledge', 'sms', 'insurance_contract', 'acct_cur',
                'conversion', 'npl', 'flag_of_strategic_client', 'zpp', 'ved', 'val_control', 'mobile_bank',
                'acquiring', 'flag_dbo', 'state_procurements', 'block_ufm_next_3_month', 'down_amt_next_month',
                'tendency_credit_products', 'tendency_zpp_products', 'tendency_acquiring_products',
                'tendency_corporate_card', 'down_active_model', 'giving_out_loan', 'receiving_loan',
                'in_transfer_own_money', 'out_transfer_own_money', 'pay_salary', ]
SUM_NOTNULL_AGGR_COLS = ['sup_manager', 'group_id', 'other_bank_name', ]
AVG_AGGR_COLS = ['life_span_in_bank', 'life_span_business', 'revenue', 'coowner_cnt', 'cnt_aff_comp', 'amount_dvs',
                'amount_dvs_mid', 'amount_deposit', 'amount_nso', 'cnt_out_transactions_per_month',
                'amt_transactions_per_month', 'cnt_transactions_in_month', 'cnt_in_transactions_per_month',
                'cash_withdrawal_amount', 'cash_input_amount', 'cash_withdrawal_atm', 'cash_input_atm',
                'cash_withdrawal_adm', 'cash_inkass', 'card_purchases', 'commission_income', 'avg_commis_income_3m',
                'revenue_mod', 'revenue_calc', 'sum_revenue_group', 'count_company_in_group', 'profit', 'loss',
                'earnings', 'num_contr', 'life_time_mod', 'ltv_rur', 'wallet_share', 'wallet_potential', ]
MODA_AGGR_COLS = ['cnt_employees_mod', 'tax_system', ]
DISTR_AGGR_COLS = ['business_category', ]
DISTR_NULL_AGGR_COLS = ['attraction_channel', ]
TOP_AGGR_COLS = ['hub_city', ]

ROUND_0_COLS = ['coowner_cnt', 'cnt_aff_comp', 'amount_dvs', 'amount_dvs_mid', 'amount_deposit', 'amount_nso',
                'cnt_out_transactions_per_month', 'amt_transactions_per_month', 'cnt_transactions_in_month',
                'cnt_in_transactions_per_month', 'cash_withdrawal_amount', 'cash_input_amount', 'cash_withdrawal_atm',
                'cash_input_atm', 'cash_withdrawal_adm', 'cash_inkass', 'card_purchases', 'commission_income',
                'avg_commis_income_3m', 'revenue_mod', 'revenue_calc', 'sum_revenue_group', 'count_company_in_group',
                'profit', 'loss', 'earnings', 'num_contr', 'life_time_mod', 'ltv_rur', ]
ROUND_1_COLS = ['life_span_in_bank', 'life_span_business', ]
ROUND_2_COLS = ['wallet_share', 'wallet_potential',]

COLUMN_VALUES_DICT = {
                        'business_category' : {
                                            'Малый' : 'small',
                                            'Средний' : 'medium'
                                            },
                        'attraction_channel' : {
                                            'Дистанционный канал' : 'distance',
                                            'Агентский канал' : 'agents',
                                            'Сетевой канал' : 'net',
                                            'Контактный центр' : 'others',
                                            'УОРС' : 'others',
                                            'Unknown' : 'unknown',
                                            },
                        }
