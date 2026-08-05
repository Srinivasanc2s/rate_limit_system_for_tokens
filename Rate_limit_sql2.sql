CREATE DATABASE IF NOT EXISTS rate_limit;
USE rate_limit;
DROP TABLE IF EXISTS users_usage; -- to store users running counter of token limits
DROP TABLE IF EXISTS query_log; -- to store the query transaction and logging and analytics
DROP TABLE IF EXISTS users; -- user table and budget limits
DROP TABLE IF EXISTS plans; -- users plan and token limits
-- -----------------------
-- PLANS TABLE
-- -----------------------
DROP TABLE IF EXISTS plans; 
CREATE TABLE plans (
    plan_id INT AUTO_INCREMENT PRIMARY KEY,
    plan_name VARCHAR(50) NOT NULL UNIQUE,
    hourly_token_limit INT DEFAULT 1000,
    daily_token_limit INT DEFAULT 10000,
    monthly_token_limit INT DEFAULT 100000,
    is_active BOOLEAN NOT NULL DEFAULT TRUE
);     
-- -----------------------
-- USERS TABLE
-- -----------------------
DROP TABLE IF EXISTS users;
CREATE TABLE users (
    user_id INT AUTO_INCREMENT PRIMARY KEY,
    user_name VARCHAR(50) NOT NULL UNIQUE,
    plan_id INT NOT NULL,
    token_budget INT  NOT NULL CHECK (token_budget > 0) DEFAULT 100, 
    remaining_budget INT  NOT NULL CHECK (remaining_budget > 0) DEFAULT 100,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    FOREIGN KEY (plan_id) REFERENCES plans(plan_id)
);
-- -----------------------
-- QUERY LOG TABLE
-- -----------------------
DROP TABLE IF EXISTS query_log; 
CREATE TABLE query_log (
    q_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT,
    plan_id INT,
    time_stamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    query_status ENUM('success', 'failed'),
    tokens_used INT,
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
    FOREIGN KEY (plan_id) REFERENCES plans(plan_id)
);
-- -----------------------
-- USERS USAGE TABLE
-- -----------------------
DROP TABLE IF EXISTS users_usage;
CREATE TABLE users_usage (
    user_id INT PRIMARY KEY,
    hourly_token INT DEFAULT 0,
    hourly_session_start DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    daily_token INT DEFAULT 0,
    daily_session_start DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    monthly_token INT DEFAULT 0,
    monthly_session_start DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
);

-- CREATE INDEX idx_querylog_user
-- ON query_log(user_id);

-- CREATE INDEX idx_querylog_time
-- ON query_log(time_stamp);

-- CREATE INDEX idx_users_plan
-- ON users(plan_id);
-- -----------------------------------------------------------------------
-- plans procedures
-- 1 a) -- sp_create_plan
DROP PROCEDURE IF EXISTS sp_create_plan;
DELIMITER $$
CREATE PROCEDURE sp_create_plan(
    IN p_name VARCHAR(50),
    IN p_hourly INT,
    IN p_daily INT,
    IN p_monthly INT
)
BEGIN
    INSERT INTO plans(
    plan_name,
    hourly_token_limit, 
    daily_token_limit, 
    monthly_token_limit
    )
    VALUES (p_name, 
    p_hourly, 
    p_daily, 
    p_monthly);
END $$
DELIMITER ;
-- ----------------------------------------------------------------------
-- 1 b) -- sp_update_plan
DROP PROCEDURE IF EXISTS sp_update_plan;
DELIMITER $$
CREATE PROCEDURE sp_update_plan(
    IN p_plan_id INT,
    IN p_hourly INT,
    IN p_daily INT,
    IN p_monthly INT,
    OUT p_message Varchar(50)
)
BEGIN
    UPDATE plans
    SET hourly_token_limit = p_hourly,
        daily_token_limit = p_daily,
        monthly_token_limit = p_monthly
    WHERE plan_id = p_plan_id
    AND is_active = TRUE;
    
        IF ROW_COUNT() = 0 THEN
        SET p_message = 'plan_is_inactive';
        ELSE
        SET p_message = 'success';
    END IF;
END $$
DELIMITER ;
-- ----------------------------------------------------------------------
-- 1 c) -- sp_soft_delete_plan
DROP PROCEDURE IF EXISTS sp_soft_delete_plan;
DELIMITER $$
CREATE PROCEDURE sp_soft_delete_plan(IN p_plan_id INT)
BEGIN
UPDATE plans
SET is_active = FALSE
WHERE plan_id = p_plan_id;
END $$
DELIMITER ;

-- 1 c2) -- sp_hard_delete optional

DROP PROCEDURE IF EXISTS sp_delete_plan;
DELIMITER $$
CREATE PROCEDURE sp_delete_plan(IN p_plan_id INT)
BEGIN 
	DELETE  FROM plans WHERE plan_id = p_plan_id;
END $$
DELIMITER ;

-- ----------------------------------------------------------------------
-- 1 d) -- sp_migrate_users_to_plan
DROP PROCEDURE IF EXISTS sp_migrate_users_to_plan;
DELIMITER $$
CREATE PROCEDURE sp_migrate_users_to_plan(
    IN p_old_plan INT,
    IN p_new_plan INT
)
BEGIN
    UPDATE users
    SET plan_id = p_new_plan
    WHERE plan_id = p_old_plan
    AND is_active = TRUE;
END $$
DELIMITER ;
-- ----------------------------------------------------------------------
-- e) sp_get_all_plan
DROP PROCEDURE IF EXISTS sp_get_all_plan;
DELIMITER $$
CREATE PROCEDURE sp_get_all_plan()
BEGIN 
	SELECT 	plan_id,plan_name,
		hourly_token_limit, 
        daily_token_limit, 
        monthly_token_limit 
        FROM plans WHERE is_active = TRUE;
END $$ 
DELIMITER ;
-- f) sp_get_plan_details
DROP PROCEDURE IF EXISTS sp_get_plan_details;
DELIMITER $$
CREATE PROCEDURE sp_get_plan_details(
 IN p_id INT)
BEGIN
SELECT plan_id,
plan_name,
hourly_token_limit,
daily_token_limit,
monthly_token_limit,
is_active FROM plans WHERE plan_id = p_id;
END $$
DELIMITER ;
-- USER Procedures --------------------------------------
-- 2 a)  sp_create_user
DROP PROCEDURE IF EXISTS sp_create_user;
DELIMITER $$
CREATE PROCEDURE sp_create_user(
    IN p_user_name VARCHAR(50),
    IN p_plan_id INT,
    IN p_budget INT
)
BEGIN
    -- INSERT INTO users Create the user
    INSERT INTO users (
        user_name,
        plan_id,
        token_budget,
        remaining_budget
    )  VALUES (
        p_user_name,
        p_plan_id,
        p_budget,
        p_budget );
	-- Initialize usage
    INSERT INTO users_usage (
        user_id,
        hourly_token,
        daily_token,
        monthly_token,
        hourly_session_start,
        daily_session_start,
        monthly_session_start
    )
    VALUES (
        LAST_INSERT_ID(),
        0,
        0,
        0,
        NOW(),
        NOW(),
        NOW()
    );
END $$;
DELIMITER ;
-- ----------------------------------------------------------------------
-- 2 b) -- sp_update_user
DROP PROCEDURE IF EXISTS sp_update_user;
DELIMITER $$
CREATE PROCEDURE sp_update_user(
    IN p_user_id INT,
    IN p_user_name VARCHAR(50),
    IN p_plan_id INT,
    IN p_budget INT
)
BEGIN
    UPDATE users
    SET plan_id = p_plan_id,
        token_budget = p_budget,
        remaining_budget = p_budget,
        user_name = p_user_name
    WHERE user_id = p_user_id AND is_active = TRUE;
END$$
DELIMITER ;
-- 2 c) -- sp_add_budget(user_id, additional_tokens)
DROP PROCEDURE IF EXISTS sp_add_budget;
DELIMITER $$
CREATE PROCEDURE sp_add_budget(
    IN p_user_id INT,
    IN additional_tokens INT
)
BEGIN
    UPDATE users
    SET 
        token_budget = token_budget + additional_tokens,
        remaining_budget = remaining_budget + additional_tokens
    WHERE user_id = p_user_id AND is_active = TRUE;
END$$
DELIMITER ;
-- ----------------------------------------------------------------------
-- 2 d) -- sp_delete_user
DROP PROCEDURE IF EXISTS sp_delete_user;
DELIMITER $$
CREATE PROCEDURE sp_delete_user(IN p_user_id INT)
BEGIN
	UPDATE users
	SET  
		is_active = FALSE
    WHERE user_id = p_user_id;
END $$
DELIMITER ;
-- ----------------------------------------------------------------------
-- 2 e) -- sp_hard_delete optional
/*
DROP PROCEDURE IF EXISTS sp_hard_delete_user;
DELIMITER $$
CREATE PROCEDURE sp_hard_delete_user(IN p_user_id INT)
BEGIN 
	DELETE  FROM users WHERE user_id = p_user_id;
END $$
DELIMITER ;
*/
-- 2 f) -- sp_get_user_details
DROP PROCEDURE IF EXISTS sp_get_user_details;
DELIMITER $$
CREATE PROCEDURE  sp_get_user_details(
IN p_user_id INT)
BEGIN
	SELECT user_id, user_name, plan_id, token_budget, remaining_budget,is_active FROM users
    WHERE user_id= p_user_id;
END $$
DELIMITER ;
-- 2 g) -- sp_list_users
DROP PROCEDURE IF EXISTS sp_list_users;
DELIMITER $$
CREATE PROCEDURE sp_list_users()
BEGIN 
	SELECT user_id, user_name,plan_id, token_budget, remaining_budget, is_active FROM users;
END $$
DELIMITER ;
-- 2 h) -- sp_get_remaining_limits
DROP PROCEDURE IF EXISTS sp_get_remaining_limits;
DELIMITER $$
CREATE PROCEDURE sp_get_remaining_limits(IN p_user_id INT)
BEGIN
    SELECT
		u.user_id,
        u.user_name,
        u.remaining_budget,
        (p.hourly_token_limit - uu.hourly_token) AS hourly_remaining,
        (p.daily_token_limit - uu.daily_token) AS daily_remaining,
        (p.monthly_token_limit - uu.monthly_token) AS monthly_remaining
    FROM users u
    JOIN users_usage uu ON u.user_id = uu.user_id
    JOIN plans p ON u.plan_id = p.plan_id
    WHERE u.user_id = p_user_id AND u.is_active = TRUE;
END $$
DELIMITER ;
-- ----------------------------------------------------------------------

-- ----------------------------------------------------------------------
-- Usage Procedures
-- 3 a) -- sp_refresh_usage
DROP PROCEDURE IF EXISTS sp_refresh_usage;
DELIMITER $$
CREATE PROCEDURE sp_refresh_usage(IN p_user_id INT)
BEGIN
    DECLARE v_now DATETIME DEFAULT NOW();
    UPDATE users_usage
    SET
        hourly_token =IF(TIMESTAMPDIFF(HOUR,hourly_session_start,v_now) >= 1,0,hourly_token),
        hourly_session_start =IF(TIMESTAMPDIFF(HOUR,hourly_session_start,v_now) >= 1,v_now,hourly_session_start),
        daily_token =IF(TIMESTAMPDIFF(HOUR,daily_session_start, v_now)>= 24,0,daily_token),
        daily_session_start = IF(TIMESTAMPDIFF(HOUR,daily_session_start,v_now) >= 24,v_now,daily_session_start),
        monthly_token = IF(TIMESTAMPDIFF(HOUR,monthly_session_start,v_now) >= 24*28,0,monthly_token),
        monthly_session_start = IF(TIMESTAMPDIFF(HOUR,monthly_session_start,v_now) >= 24*28,v_now,monthly_session_start)
		WHERE user_id = p_user_id;
END $$
DELIMITER ;
-- ----------------------------------------------------------------------
-- 3 b)-- sp_get_current_usage
DROP PROCEDURE IF EXISTS sp_get_current_usage;
DELIMITER $$
CREATE PROCEDURE sp_get_current_usage(IN p_user_id INT)
BEGIN
    SELECT user_id,hourly_token,hourly_session_start,daily_token, daily_session_start, monthly_token, monthly_session_start 
    FROM users_usage WHERE user_id = p_user_id;
END $$
DELIMITER ;
-- ----------------------------------------------------------------------
-- 3 c)-- sp_get_all_usage
DROP PROCEDURE IF EXISTS sp_get_all_usage;
DELIMITER $$
CREATE PROCEDURE sp_get_all_usage()
BEGIN
    SELECT user_id,hourly_token,hourly_session_start,daily_token, daily_session_start, monthly_token, monthly_session_start FROM users_usage ;
END $$
DELIMITER ;
-- ----------------------------------------------------------------------
-- VALIDATION
-- ----------------------------------------------------------------------
-- 4 a) sp_log_query
DROP PROCEDURE IF EXISTS sp_log_query;
DELIMITER $$

CREATE PROCEDURE sp_log_query(
    IN p_user_id INT,
    IN p_plan_id INT,
    IN p_status VARCHAR(20),
    IN p_tokens INT
)
BEGIN
    INSERT INTO query_log(
        user_id,
        plan_id,
        query_status,
        tokens_used
    )
    VALUES(
        p_user_id,
        p_plan_id,
        p_status,
        p_tokens
    );
END $$
DELIMITER ;
-- 4 b) sp_validate_request
DROP PROCEDURE IF EXISTS sp_validate_request;
DELIMITER $$
CREATE PROCEDURE sp_validate_request(
    IN p_user_id INT,
    IN p_allow_budget BOOLEAN,
    OUT p_message VARCHAR(100)
)
BEGIN
    DECLARE v_remaining INT;
    DECLARE v_total_budget INT;
    DECLARE v_plan_id INT;
    DECLARE v_hourly INT;
    DECLARE v_daily INT;
    DECLARE v_monthly INT;
    DECLARE v_hourly_limit INT;
    DECLARE v_daily_limit INT;
    DECLARE v_monthly_limit INT;
    DECLARE v_limit_msg VARCHAR(100);
    CALL sp_refresh_usage(p_user_id);
    SELECT
        remaining_budget,
        token_budget,
        plan_id
    INTO
        v_remaining,
        v_total_budget,
        v_plan_id
    FROM users
    WHERE user_id = p_user_id
      AND is_active = TRUE;
    SELECT
        uu.hourly_token,
        uu.daily_token,
        uu.monthly_token,
        p.hourly_token_limit,
        p.daily_token_limit,
        p.monthly_token_limit
    INTO
        v_hourly,
        v_daily,
        v_monthly,
        v_hourly_limit,
        v_daily_limit,
        v_monthly_limit
    FROM users_usage uu
    JOIN users u
      ON uu.user_id = u.user_id
    JOIN plans p
      ON u.plan_id = p.plan_id
    WHERE uu.user_id = p_user_id;
    SET v_limit_msg = '';
    IF v_hourly >=  v_hourly_limit THEN
        SET v_limit_msg = CONCAT(v_limit_msg,'HOURLY ');
    END IF;
    IF v_daily >=  v_daily_limit THEN
        SET v_limit_msg = CONCAT(v_limit_msg,'DAILY ');
    END IF;
    IF v_monthly >=  v_monthly_limit THEN
        SET v_limit_msg = CONCAT(v_limit_msg,'MONTHLY ');
    END IF;
    IF v_limit_msg = '' THEN
        SET p_message='ALLOWED';
    ELSE
        IF p_allow_budget = TRUE THEN
            IF v_remaining >= 0.02 * v_total_budget THEN
                SET p_message='ALLOWED_USING_BUDGET';
            ELSE
                CALL sp_log_query(
                    p_user_id,
                    v_plan_id,
                    'failed',
                    0
                );
                SET p_message=CONCAT(TRIM(v_limit_msg),'_LIMIT_EXCEEDED + INSUFFICIENT_BUDGET');
            END IF;
        ELSE
            CALL sp_log_query(
                p_user_id,
                v_plan_id,
                'failed',
                0
            );
            SET p_message=CONCAT(TRIM(v_limit_msg),'_LIMIT_EXCEEDED');
        END IF;
    END IF;
END$$
DELIMITER ;
-- ----------------------------------------------------------------------
-- 4 c) sp_record_usage
DROP PROCEDURE IF EXISTS sp_record_usage;
DELIMITER $$
CREATE PROCEDURE sp_record_usage(
    IN p_user_id INT,
    IN p_tokens INT,
    IN p_allow_budget BOOLEAN,
    OUT p_message VARCHAR(100)
)
main: BEGIN
    DECLARE v_plan_id INT;
    DECLARE v_remaining INT;
    DECLARE v_hourly INT;
    DECLARE v_daily INT;
    DECLARE v_monthly INT;
    DECLARE v_hourly_limit INT;
    DECLARE v_daily_limit INT;
    DECLARE v_monthly_limit INT;
    DECLARE add_tokens_hourly INT;
    DECLARE add_tokens_daily INT;
    DECLARE add_tokens_monthly INT;
    DECLARE max_token_used INT;
    DECLARE v_overflow BOOLEAN DEFAULT FALSE;
    START TRANSACTION;
    SET max_token_used =0,
    add_tokens_hourly = 0,
    add_tokens_daily = 0,
    add_tokens_monthly = 0;
    SELECT
        u.plan_id,
        u.remaining_budget,
        uu.hourly_token,
        uu.daily_token,
        uu.monthly_token,
        p.hourly_token_limit,
        p.daily_token_limit,
        p.monthly_token_limit
    INTO
        v_plan_id,
        v_remaining,
        v_hourly,
        v_daily,
        v_monthly,
        v_hourly_limit,
        v_daily_limit,
        v_monthly_limit
    FROM users u
    JOIN users_usage uu
      ON u.user_id = uu.user_id
    JOIN plans p
      ON u.plan_id = p.plan_id
    WHERE u.user_id = p_user_id
    FOR UPDATE;
    IF (v_hourly + p_tokens > v_hourly_limit) THEN 
    SET v_overflow = TRUE,
		add_tokens_hourly = v_hourly + p_tokens - v_hourly_limit;
	END IF;
    IF (v_daily + p_tokens > v_daily_limit) THEN 
        SET v_overflow = TRUE,
		add_tokens_daily = v_daily + p_tokens - v_daily_limit;
	END IF;
	IF (v_monthly + p_tokens > v_monthly_limit) THEN 
        SET v_overflow = TRUE,
		add_tokens_monthly = v_monthly + p_tokens - v_monthly_limit;
    END IF;
    SET max_token_used = GREATEST(add_tokens_hourly , add_tokens_daily , add_tokens_monthly);
    UPDATE users_usage
    SET
        hourly_token = hourly_token + p_tokens - add_tokens_hourly,
        daily_token = daily_token + p_tokens- add_tokens_daily,
        monthly_token = monthly_token + p_tokens - add_tokens_monthly
        
    WHERE user_id = p_user_id;
    IF v_overflow = TRUE THEN
        IF p_allow_budget = TRUE THEN
            IF v_remaining >= max_token_used THEN
                UPDATE users
                SET remaining_budget = remaining_budget - max_token_used
                WHERE user_id = p_user_id;
                SET p_message='SUCCESS_USING_OVERFLOW';
            ELSE
                ROLLBACK;
                SET p_message='INSUFFICIENT_BUDGET';
                LEAVE main;
            END IF;
        ELSE
            ROLLBACK;
            SET p_message='PLAN_LIMIT_EXCEEDED';
            LEAVE main;
        END IF;
    ELSE
        SET p_message='SUCCESS';
    END IF;
    CALL sp_log_query(
        p_user_id,
        v_plan_id,
        'success',
        p_tokens
    );
    COMMIT;
END$$
DELIMITER ;
-- ----------------------------------------------------------------------
-- 5 a)-- sp_get_query_history
DROP PROCEDURE IF EXISTS sp_get_query_history;
DELIMITER $$
CREATE PROCEDURE sp_get_query_history(IN p_user_id INT)
BEGIN
    SELECT * FROM query_log
    WHERE user_id = p_user_id
    ORDER BY time_stamp DESC;
END $$
DELIMITER ;
-- -------------------------------------------------------------------------
-- Test cases
SELECT * FROM users;
select * from users_usage;
select * from plans;
SELECT * FROM query_log;
SELECT * FROM request_log;
-- Insert plans
-- 1) valid plans
CALL sp_create_plan('Basic',1000,25000,100000); -- valid plan
CALL sp_create_plan('plus', 5000,125000,500000);-- valid plan
CALL sp_create_plan('pro',10000,250000,1000000);-- valid plan
-- 2) Duplicate plan
CALL sp_create_plan('Basic',10000,25000,100000) ; -- Duplicate plan_name
-- 3) Null plan_name
CALL sp_create_plan(1000,25000,100000);
CALL sp_create_plan(NULL, 1000, 25000, 100000);
-- 4) negative token limit
ALTER TABLE plans
ADD CONSTRAINT chk_hourly_token_limit
    CHECK (hourly_token_limit > 0),
ADD CONSTRAINT chk_daily_token_limit
    CHECK (daily_token_limit > 0),
ADD CONSTRAINT chk_monthly_token_limit
    CHECK (monthly_token_limit > 0);
CALL sp_create_plan('NegativePlan',-100,10000,100000);
-- 5) update existing plan
CALL sp_update_plan(1,2000,50000,200000,@message);
SELECT @message;
-- 6) update inactive plan
CALL sp_soft_delete_plan(1);
CALL sp_update_plan(1,3000,60000,300000,@message);
SELECT @message;
-- 7) test get all plan
CALL sp_get_all_plan() ;
-- 8) sp_get_plan_details
CALL sp_get_plan_details(2);
CALL sp_get_plan_details(1);
-- users procedures ---------------------------------
-- 9) Create users
CALL sp_create_user('john',1,1000);
CALL sp_create_user('cena',1,2000);
CALL sp_create_user('Alice',2,5000);
CALL sp_create_user('Brad',3,7000);
CALL sp_create_user('Dan',3,10000);
CALL sp_create_user('Ethan',3,8000);
CALL sp_create_user('Bob',999,1000); -- invalid plan foreign key constraint
-- 10) update users
CALL sp_update_user(2,'caren',1,1000);
CALL sp_update_user(8,'AlexNew',1,1000);
-- 11) add_budget
CALL sp_add_budget(3,1000);
-- 12) sp_delete_user (soft)
CALL sp_delete_user(2);
-- 13) sp_get_user_details
CALL sp_get_user_details(2);
CALL sp_get_user_details(3);
CALL sp_get_user_details(1);
-- 14) sp_list_users
CALL sp_list_users();
-- 15) sp_get_remaining_limits
CALL sp_get_remaining_limits(2);
CALL sp_get_remaining_limits(3);
CALL sp_get_remaining_limits(1);
-- 16) midgrate users to plan
CALL sp_migrate_users_to_plan(1,2);
CALL sp_list_users();
-- 17) get_current_usage
CALL sp_get_current_usage(3);
-- 18) sp_get_all_usage
CALL sp_get_all_usage();
-- 19) sp_refresh_usage
CALL sp_refresh_usage(3);
-- 20)sp_log_query
CALL sp_log_query(2,1,'success',125);
-- 21) sp_get_query_history
CALL sp_get_query_history(1);
-- 21) sp_hard_delete_user
CALL sp_hard_delete_user(2); -- does not exist
-- 22) sp_validate_request
UPDATE users_usage
SET hourly_token=0
WHERE user_id=1;
CALL sp_validate_request(1,FALSE,@msg);
SELECT @msg;
CALL sp_validate_request(3,@msg);
SELECT @msg;
-- 23) SP_RECORD_USAGE
CALL sp_record_usage( 1,100,FALSE,@msg);
SELECT @msg;
-- excess token
CALL sp_validate_request(1,FALSE,@msg);
SELECT @msg;
CALL sp_record_usage( 1,5200,FALSE,@msg);
SELECT @msg;
CALL sp_validate_request(1,TRUE,@msg);
SELECT @msg;
CALL sp_record_usage( 1,5200,TRUE,@msg);
SELECT @msg;
-- CALL sp_record_usage( 1,999999,TRUE,@msg);
CALL sp_get_all_usage();
CALL sp_get_query_history(1);
-- ------------------------------------------------------
-- VERSION 2
-- ------------------------------------------------------

-- Add new columns to query_log for chat messages
ALTER TABLE query_log ADD COLUMN (
    conversation_id VARCHAR(100),
    user_message VARCHAR(1000),
    llm_response VARCHAR(3000),
    message_type ENUM('llm_query', 'validation_check', 'budget_check', 'procedure_exec') DEFAULT 'llm_query',
    allow_budget_override BOOLEAN DEFAULT FALSE,
    validation_result VARCHAR(100),
    tokens_estimated INT,
    execution_time_ms INT,
    is_successful BOOLEAN DEFAULT TRUE
);

	
-- Add index for fast chat retrieval
CREATE INDEX idx_chat_history ON query_log(user_id, conversation_id, time_stamp);

SHOW COLUMNS FROM query_log;

-- CREATE TABLE request log for the tracing and logging backend
CREATE TABLE request_log (
    log_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    
    request_id VARCHAR(100) UNIQUE,
    user_id INT,
    http_method VARCHAR(10),
    endpoint VARCHAR(300),
    
    request_params JSON,
    query_string VARCHAR(500),
    
    response_status_code INT,
    response_message VARCHAR(500),
    response_data_size_bytes INT,
    
    backend_trace JSON,
    
    execution_time_ms INT,
    db_time_ms INT,
    
    client_ip VARCHAR(45),
    user_agent VARCHAR(300),
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_user_time (user_id, timestamp),
    INDEX idx_status_code (response_status_code),
    INDEX idx_endpoint (endpoint),
    INDEX idx_request_id (request_id),
    INDEX idx_timestamp (timestamp)
);

ALTER TABLE request_log 
DROP column user_agent, 
DROP COLUMN db_time_ms ,
DROP COLUMN query_string,
DROP COLUMN response_data_size_bytes;

DESCRIBE query_log;  -- Should show the 9 new columns
DESCRIBE request_log;  -- Should show all logging columns
SHOW INDEX FROM query_log;  -- Should see idx_chat_history
SELECT * FROM request_log;
SELECT * FROM query_log;
SELECT @@port;
SELECT @@hostname;
SELECT VERSION();


-- 5 b) sp_log_request
DROP PROCEDURE IF EXISTS sp_log_request;
DELIMITER $$

CREATE PROCEDURE sp_log_request(
    IN p_request_id VARCHAR(100),
    IN p_user_id INT,
    IN p_http_method VARCHAR(10),
    IN p_endpoint VARCHAR(300),
    IN p_request_params JSON,
    IN p_response_status_code INT,
    IN p_response_message VARCHAR(500),
    IN p_backend_trace JSON,
    IN p_execution_time_ms INT,
    IN p_client_ip VARCHAR(45)
)
BEGIN

    INSERT INTO request_log(
        request_id,
        user_id,
        http_method,
        endpoint,
        request_params,
        response_status_code,
        response_message,
        backend_trace,
        execution_time_ms,
        client_ip
    )
    VALUES(
        p_request_id,
        p_user_id,
        p_http_method,
        p_endpoint,
        p_request_params,
        p_response_status_code,
        p_response_message,
        p_backend_trace,
        p_execution_time_ms,
        p_client_ip
    );

END$$
DELIMITER ;

-- 5c) sp_log_query_v2
DROP PROCEDURE IF EXISTS sp_log_query_v2;
DELIMITER $$

CREATE PROCEDURE sp_log_query_v2(
    IN p_user_id INT,
    IN p_plan_id INT,
    IN p_query_status VARCHAR(20),
    IN p_tokens_used INT,

    IN p_conversation_id VARCHAR(100),
    IN p_user_message VARCHAR(1000),
    IN p_llm_response VARCHAR(3000),

    IN p_message_type VARCHAR(30),

    IN p_allow_budget_override BOOLEAN,
    IN p_validation_result VARCHAR(100),

    IN p_execution_time_ms INT,

    IN p_is_successful BOOLEAN
)
BEGIN

    INSERT INTO query_log(

        user_id,
        plan_id,
        query_status,
        tokens_used,

        conversation_id,
        user_message,
        llm_response,
        message_type,

        allow_budget_override,
        validation_result,

        execution_time_ms,
        is_successful

    )
    VALUES(

        p_user_id,
        p_plan_id,
        p_query_status,
        p_tokens_used,

        p_conversation_id,
        p_user_message,
        p_llm_response,
        p_message_type,

        p_allow_budget_override,
        p_validation_result,

        p_execution_time_ms,
        p_is_successful

    );

END$$
DELIMITER ;

-- 5d) sp_get_chat_history
 
 DROP PROCEDURE IF EXISTS sp_get_chat_history;
DELIMITER $$

CREATE PROCEDURE sp_get_chat_history(
    IN p_user_id INT
)
BEGIN

    SELECT

        q_id,
        conversation_id,
        user_message,
        llm_response,
        tokens_used,
        validation_result,
        query_status,
        time_stamp

    FROM query_log

    WHERE user_id = p_user_id
      AND message_type='llm_query'

    ORDER BY time_stamp DESC

    LIMIT 100;

END$$
DELIMITER ;

-- 5 e) sp_get_request_logs
DROP PROCEDURE IF EXISTS sp_get_request_logs;
DELIMITER $$

CREATE PROCEDURE sp_get_request_logs(
    IN p_user_id INT,
    IN p_limit INT
)
BEGIN

    IF p_user_id IS NULL THEN

        IF p_limit IS NULL THEN

            SELECT *
            FROM request_log
            ORDER BY timestamp DESC;

        ELSE

            SELECT *
            FROM request_log
            ORDER BY timestamp DESC
            LIMIT p_limit;

        END IF;

    ELSE

        IF p_limit IS NULL THEN

            SELECT *
            FROM request_log
            WHERE user_id=p_user_id
            ORDER BY timestamp DESC;

        ELSE

            SELECT *
            FROM request_log
            WHERE user_id=p_user_id
            ORDER BY timestamp DESC
            LIMIT p_limit;

        END IF;

    END IF;

END$$
DELIMITER ;

