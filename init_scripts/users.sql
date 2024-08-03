-- User

SELECT md5('pwd' || 'username')

ALTER USER username ALTER PASSWORD 'md5...'

--===============
-- Data Team Members (Operation)
-- Create User
CREATE USER <username> PASSWORD '' NOCREATEDB NOCREATEUSER SYSLOG ACCESS UNRESTRICTED;

-- Add to group
ALTER GROUP g_datateam ADD USER <username>;

-- Delegate
ALTER DEFAULT PRIVILEGES FOR USER <username>         GRANT ALL PRIVILEGES ON TABLES     TO GROUP g_datateam;
ALTER DEFAULT PRIVILEGES FOR USER <username>         GRANT ALL PRIVILEGES ON FUNCTIONS  TO GROUP g_datateam;
ALTER DEFAULT PRIVILEGES FOR USER <username>         GRANT ALL PRIVILEGES ON PROCEDURES TO GROUP g_datateam;


--===============
-- Data Team Worker
-- User for ETL tasks
-- Create User
CREATE USER data_worker PASSWORD '' NOCREATEDB NOCREATEUSER;

-- Add to group
ALTER GROUP g_etl_worker ADD USER data_worker;

--===============
-- Power BI
-- User to use on Power BI
-- Create User
CREATE USER powerbi PASSWORD '' NOCREATEDB NOCREATEUSER;

-- Add to group
ALTER GROUP g_bi_readall ADD USER powerbi;