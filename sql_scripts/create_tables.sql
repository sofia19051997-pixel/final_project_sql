create schema final_project;
set search_path to final_project;


---- STG-таблицы
create table if not exists final_project.STG_transactions (
    transaction_id varchar(150),
    transaction_date timestamp,
    amount varchar(50),
    card_num varchar(50),
    oper_type varchar(150),
    oper_result varchar(150),
    terminal varchar(150)
);

create table if not exists STG_terminals (
    terminal_id varchar(150),
    terminal_type varchar(150),
    terminal_city varchar(150),
    terminal_address varchar(250)
);


create table if not exists STG_passport_blacklist (
    date date,
    passport varchar(50)
);


create table if not exists STG_clients (
    client_id varchar(128), 
    last_name varchar(128), 
    first_name varchar(128), 
    patronymic varchar(128), 
    date_of_birth date, 
    passport_num varchar(128), 
    passport_valid_to date, 
    phone varchar(128),
    create_dt date, 
    update_dt date
);


create table if not exists STG_cards (
    card_num varchar(128), 
	account varchar(128), 
	create_dt date,
	update_dt date
);


create table if not exists STG_accounts (
    account varchar(128), 
	valid_to date, 
	client varchar(128),
	create_dt date, 
	update_dt date
);


---- DWH-таблицы

create table if not exists DWH_FACT_transactions (
    transaction_id varchar(150),
    transaction_date timestamp,
    amount varchar(50),
    card_num varchar(50),
    oper_type varchar(150),
    oper_result varchar(150),
    terminal varchar(150),
    create_dt date
);


create table if not exists DWH_FACT_passport_blacklist (
    date date,
    passport varchar(50),
    effective_from date not null,
    effective_to date not null default '5999-12-31',
    primary key (passport, effective_from)
);

create table if not exists DWH_DIM_clients (
    client_id varchar(50) primary key,
    last_name varchar(150),
    first_name varchar(150),
    patronymic varchar(150),
    date_of_birth date,
    passport_num varchar(50),
    passport_valid_to date,
    phone varchar(50)
);

create table if not exists DWH_DIM_cards (
    card_num varchar(50) primary key,
    account_num varchar(150)
);

create table if not exists DWH_DIM_accounts (
    account_num varchar(150) primary key,
    client varchar(50),
    valid_to date
);

create table if not exists DWH_DIM_terminals_HIST (
    terminal_id varchar(150),
    terminal_type varchar(150),
    terminal_city varchar(150),
    terminal_address varchar(250),
    effective_from date not null,
    effective_to date not null default '5999-12-31',
    deleted_flg boolean default false,
    primary key (terminal_id, effective_from)
);



---- Таблица с отчетом 

create table if not exists REP_FRAUD (
    id serial primary key,
    event_dt timestamp not null,
    passport varchar(50) not null,
    fio varchar(250),
    phone varchar(50),
    event_type varchar(200) not null,
    report_dt date not null
);


---- таблицы для метаданных

create table if not exists META_load_files (
    id serial primary key,
    file_name varchar(200) not null,
    file_date date not null,
    load_dt timestamp default current_timestamp,
    processed_flg boolean default false,
    backup_path varchar(200)
);

---- индексы
create index if not exists idx_rep_fraud_report_dt on REP_FRAUD(report_dt);
create index if not exists  idx_rep_fraud_passport on REP_FRAUD(passport);




