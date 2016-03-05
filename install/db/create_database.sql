-- Creates database roles and database for this project
-- XXX Must be superuser to run!

-- Do not use same name for user and a schema.
-- That schema then will be in search path, which leads to idiosyncrasies.
CREATE ROLE pym_user LOGIN ENCRYPTED PASSWORD 'pym';

CREATE DATABASE stoma OWNER pym_user ENCODING 'utf-8';
