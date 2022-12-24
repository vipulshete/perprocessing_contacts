-- change password (no password)

sudo mysql -u root
USE mysql;
SELECT User, Host, plugin FROM mysql.user;
UPDATE user SET plugin='mysql_native_password' WHERE User='root';
FLUSH PRIVILEGES;
exit;

-- Create database and table

CREATE database address_book;
 
Use address_book;

CREATE TABLE contacts(
id TEXT,
first_name TEXT,
last_name TEXT,
city TEXT,
countery_code_with_mobile_no TEXT,
email TEXT,
Is_mail_send int,
Stream TEXT,
Type_of_strem TEXT);
 
Show tables;
 
Describe table contacts;