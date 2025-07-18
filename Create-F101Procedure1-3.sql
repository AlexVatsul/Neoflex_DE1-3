create table dm.dm_f101_round_f (
    from_date date,
    to_date date,
    chapter char(1),
    ledger_account char(5),
    characteristic char(1),
    balance_in_rub numeric(23,8),
    balance_in_val numeric(23,8),
    balance_in_total numeric(23,8),
    turn_deb_rub numeric(23,8),
    turn_deb_val numeric(23,8),
    turn_deb_total numeric(23,8),
    turn_cre_rub numeric(23,8),
    turn_cre_val numeric(23,8),
    turn_cre_total numeric(23,8),
    balance_out_rub numeric(23,8),
    balance_out_val numeric(23,8),
    balance_out_total numeric(23,8)
);


truncate dm.dm_f101_round_f;


create or replace procedure dm.fill_f101_round_f(i_ondate date)
language plpgsql
as $$
declare
	start_date timestamp := clock_timestamp();
	f_from_date date;
	f_to_date date;

    account_ids bigint[];
    current_balance integer;

	f_chapter char(1);
	f_characteristic char(1);
    
   	f_balance_in_rub numeric(23,8);
	f_balance_in_val numeric(23,8);
	f_balance_in_total numeric(23,8);
	f_turn_deb_rub numeric(23,8);
	f_turn_deb_val numeric(23,8);
	f_turn_deb_total numeric(23,8);
	f_turn_cre_rub numeric(23,8);
	f_turn_cre_val numeric(23,8);
	f_turn_cre_total numeric(23,8);
	f_balance_out_rub numeric(23,8);
	f_balance_out_val numeric(23,8);
	f_balance_out_total numeric(23,8);

begin
    -- устанавливаем даты
    f_from_date := i_ondate - interval '1 month';
    f_to_date := i_ondate - interval '1 day';

    -- цикл по уникальным balance_2
    for current_balance in 
        select distinct substring(account_number from 1 for 5)::integer
        from ds.md_account_d
        order by 1
    loop
        -- получаем все account_rk для текущего balance_2
        select array_agg(account_rk) into account_ids
        from ds.md_account_d
        where substring(account_number from 1 for 5)::integer = current_balance;
        
        raise notice 'обрабатываем balance_2: %, account_rk: %', current_balance, account_ids;
        
        -- здесь будут расчеты для каждого balance_2
		
		-- chapter
		select chapter into f_chapter
		from ds.md_ledger_account_s mlas
		where mlas.ledger_account = current_balance;


		raise notice 'chapter: %', f_chapter;


		with ds_with_balance2 as (
			select data_actual_date, data_actual_end_date, account_rk,
			substring(mad.account_number from 1 for 5)::integer as balance_2,
			char_type, currency_rk, currency_code
			from ds.md_account_d mad	
		)
		select distinct ds2.char_type into f_characteristic
		from ds_with_balance2 ds2
		left join ds.md_ledger_account_s mlas on mlas.ledger_account = ds2.balance_2
		where ds2.balance_2 = current_balance;

		
		raise notice 'characteristic: %', f_characteristic;				
		
		
		-- balance_in_rub +
		select sum(dabf.balance_out_rub) into f_balance_in_rub
		from dm.dm_account_balance_f dabf
		left join ds.md_account_d ma on ma.account_rk = dabf.account_rk
		where dabf.on_date = f_from_date - interval '1 day'
			and ma.account_rk = ANY(account_ids)
			and ma.currency_code in ('643', '810');


		raise notice 'from_date: %', f_from_date;
		raise notice 'to_date: %', f_to_date;
		raise notice 'balance_in_rub: %', f_balance_in_rub;
	

		-- balance_in_val +
		select sum(dabf.balance_out_rub) into f_balance_in_val
		from dm.dm_account_balance_f dabf
		left join ds.md_account_d ma on ma.account_rk = dabf.account_rk
		where dabf.on_date = f_from_date - interval '1 day'
			and ma.account_rk = ANY(account_ids)
			and ma.currency_code not in ('643', '810');


		-- balance_in_total +
		f_balance_in_total := f_balance_in_rub + f_balance_in_val;


		raise notice 'balance_in_val: %', f_balance_in_val;
		raise notice 'balance_in_total: %', f_balance_in_total;


		-- turn_deb_rub +
		select sum(datf.debet_amount_rub) into f_turn_deb_rub
		from dm.dm_account_turnover_f datf
		left join ds.md_account_d ma on ma.account_rk = datf.account_rk
		where ma.currency_code in ('643', '810')
			and ma.account_rk = ANY(account_ids);

		raise notice 'turn_deb_rub: %', f_turn_deb_rub;

		-- turn_deb_val +
		select sum(datf.debet_amount_rub) into f_turn_deb_val
		from dm.dm_account_turnover_f datf
		left join ds.md_account_d ma on ma.account_rk = datf.account_rk
		where ma.currency_code not in ('643', '810')
			and ma.account_rk = ANY(account_ids);

		-- turn_deb_total +
		f_turn_deb_total := f_turn_deb_val + f_turn_deb_rub;

		raise notice 'turn_deb_val: %', f_turn_deb_val;
		raise notice 'turn_deb_total: %', f_turn_deb_total;	
	

		-- turn_cre_rub +
		select sum(datf.credit_amount_rub) into f_turn_cre_rub
		from dm.dm_account_turnover_f datf 
		left join ds.md_account_d ma on ma.account_rk = datf.account_rk
		where ma.currency_code in ('643', '810')
			and ma.account_rk = ANY(account_ids);

		raise notice 'turn_cre_rub: %', f_turn_cre_rub;


		-- turn_cre_val +
		select sum(datf.credit_amount_rub) into f_turn_cre_val
		from dm.dm_account_turnover_f datf 
		left join ds.md_account_d ma on ma.account_rk = datf.account_rk
		where ma.currency_code not in ('643', '810')
			and ma.account_rk = ANY(account_ids);

		-- turn_cre_total +
		f_turn_cre_total := f_turn_cre_val + f_turn_cre_rub;

		raise notice 'turn_cre_val: %', f_turn_cre_val;	
		raise notice 'turn_cre_total: %', f_turn_cre_total;	
	

		-- balance_out_rub +
		select sum(dabf.balance_out_rub) into f_balance_out_rub
		from dm.dm_account_balance_f dabf 
		left join ds.md_account_d mad on dabf.account_rk = mad.account_rk
		where mad.currency_code in ('643', '810')
			and dabf.on_date = f_to_date
			and mad.account_rk = ANY(account_ids);

		raise notice 'balance_out_rub: %', f_balance_out_rub;
	

		-- balance_out_val +
		select sum(dabf.balance_out_rub) into f_balance_out_val
		from dm.dm_account_balance_f dabf 
		left join ds.md_account_d mad on dabf.account_rk = mad.account_rk
		where mad.currency_code not in ('643', '810')
			and dabf.on_date = f_to_date
			and mad.account_rk = ANY(account_ids);

		-- balance_out_total
		f_balance_out_total := f_balance_out_rub + f_balance_out_val;
	
		raise notice 'balance_out_val: %', f_balance_out_val;	
		raise notice 'balance_out_total: %', f_balance_out_total;	
		


		insert into dm.dm_f101_round_f (from_date, to_date, chapter, ledger_account,
					characteristic, balance_in_rub, balance_in_val, balance_in_total,
					turn_deb_rub, turn_deb_val, turn_deb_total, turn_cre_rub, turn_cre_val,
					turn_cre_total, balance_out_rub, balance_out_val, balance_out_total)
		values (f_from_date, f_to_date, f_chapter, current_balance,
					f_characteristic, f_balance_in_rub, f_balance_in_val, f_balance_in_total,
					f_turn_deb_rub, f_turn_deb_val, f_turn_deb_total, f_turn_cre_rub, f_turn_cre_val,
					f_turn_cre_total, f_balance_out_rub, f_balance_out_val, f_balance_out_total);

    end loop;
end;
$$;


call dm.fill_f101_round_f('2018-02-01');


truncate dm.dm_f101_round_f;

select *
from dm.dm_f101_round_f dfrf
order by dfrf.ledger_account;



















