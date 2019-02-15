--liquibase formatted sql

--changeset danmoran:01
CREATE TABLE queues (
    name varchar(250) PRIMARY KEY,
    request_topic varchar(250) NOT NULL,
    response_topic varchar(250) NOT NULL,
    request_schema json NOT NULL
);
--rollback drop table queues;
