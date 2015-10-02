SET SYSTEM PROPERTY MAX_CONNECTION_OPEN_STATEMENTS=15000;
SET SYSTEM PROPERTY MAX_CONNECTION_OPEN_RESULTSETS=15000;
create table ${db.schema}."OWNER" (id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, customerId BIGINT, ownerGuid STRING, dateCreated DATE, lastUpdated DATE, name STRING, masterAliasId BIGINT, region STRING);
create table ${db.schema}."EVENT" (id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, customerId BIGINT, ownerId BIGINT, eventGuid STRING, name STRING, description STRING, dateCreated DATE, lastUpdated DATE, region STRING);
create table ${db.schema}."GROUP" (id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, eventId BIGINT, groupGuid STRING, description STRING, dataCount INTEGER, dateCreated DATE, lastUpdated DATE, region STRING, week BIGINT);
create table ${db.schema}."DATA"  (id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, groupId BIGINT, dataGuid String, instanceUID STRING, createdDateTime TIMESTAMP(0), acquiredDateTime TIMESTAMP(0), version INT, active BOOLEAN, sizeOnDiskMB DECIMAL(6,2), regionWeek STRING);
CREATE INDEX groupLookup on ${db.schema}."DATA" (groupId);
CREATE INDEX eventLookup on ${db.schema}."GROUP" (eventId);
create procedure ${db.schema}.${sp.name.prefix}InsertOwner (OUT "@id" BIGINT, $customerId BIGINT, $ownerGuid STRING, $createdDate DATE, $lastUpdated DATE, $name STRING, $masterAliasId BIGINT, $region STRING)
 AS
  VAR $ownerId BIGINT = (SELECT id from ${db.schema}."OWNER" where customerId = $customerId AND ownerGuid = $ownerGuid);
  IF ($ownerId IS NULL)
FOR INSERT into ${db.schema}."OWNER" (customerId, ownerGuid, dateCreated, lastUpdated, name, masterAliasId, region)
 VALUES ($customerId, $ownerGuid||'-SP', NOW(), NOW(), $name, $masterAliasId, $region);
 "@id" = id;
END_FOR;
  ELSE
UPDATE ${db.schema}."OWNER" SET lastUpdated = (NOW()) where id = $ownerId;
"@id" = $ownerId;
  END_IF;
 END_PROCEDURE;
create procedure ${db.schema}.${sp.name.prefix}InsertEvent (OUT "@id" BIGINT, $customerId BIGINT, $ownerId BIGINT, $eventGuid STRING, $name STRING, $description STRING, $createdDate DATE, $lastUpdated DATE, $region STRING)
 AS
  VAR $eventId BIGINT = (SELECT id from ${db.schema}."EVENT" where customerId = $customerId AND ownerId = $ownerId AND eventGuid = $eventGuid);
  IF ($eventId IS NULL)
FOR INSERT into ${db.schema}."EVENT" (customerId, ownerId, eventGuid, name, description, dateCreated, lastUpdated, region)
 VALUES ($customerId, $ownerId, $eventGuid||'-SP', $name, $description, NOW(), NOW(), $region);
 "@id" = id;
END_FOR;
  ELSE
UPDATE ${db.schema}."EVENT" SET lastUpdated = (NOW()) where id = $ownerId;
"@id" = $eventId;
  END_IF;
 END_PROCEDURE;
create procedure ${db.schema}.${sp.name.prefix}InsertGroup (OUT "@id" BIGINT, $eventId BIGINT, $groupGuid STRING, $description STRING, $dataCount INT, $dateCreated DATE, $lastUpdated DATE, $region STRING, $week BIGINT)
 AS
  VAR $groupId BIGINT = (SELECT id from ${db.schema}."GROUP" where eventId = $eventId AND groupGuid = $groupGuid);
  IF ($groupId IS NULL)
FOR INSERT into ${db.schema}."GROUP" (eventId, groupGuid, description, dataCount, dateCreated, lastUpdated, region, week)
 VALUES ($eventId, $groupGuid||'-SP', $description, $dataCount, NOW(), NOW(), $region, $week);
 "@id" = id;
END_FOR;
  ELSE
UPDATE ${db.schema}."GROUP" SET lastUpdated = (NOW()) where id = $groupId;
"@id" = $groupId;
  END_IF;
 END_PROCEDURE;
create procedure ${db.schema}.${sp.name.prefix}InsertData (OUT "@id" BIGINT, $groupId BIGINT, $dataGuid STRING, $instanceUID STRING, $createdDateTime TIMESTAMP, $acquiredDateTime TIMESTAMP, $version INT, $active BOOLEAN, $sizeOnDiskMB DECIMAL, $regionWeek STRING)
 AS
  VAR $dataId BIGINT;
  VAR $currVersion DECIMAL;
  $dataId, $currVersion = (SELECT id, version from ${db.schema}."DATA" where groupId = $groupId AND dataGuid = $dataGuid AND active = '1');
  //$dataId = NULL;
  IF ($dataId IS NULL)
INSERT into ${db.schema}."DATA" (groupId, dataGuid, instanceUID, createdDateTime, acquiredDateTime, version, active, sizeOnDiskMB, regionWeek)
 VALUES ($groupId, $dataGuid||'-SP', $instanceUID, $createdDateTime, $acquiredDateTime, 1, 1, $sizeOnDiskMB, $regionWeek);
  ELSE
IF ($currVersion < 255)
 INSERT into ${db.schema}."DATA" (groupId, dataGuid, instanceUID, createdDateTime, acquiredDateTime, version, active, sizeOnDiskMB, regionWeek)
  VALUES ($groupId, $dataGuid, $instanceUID, $createdDateTime, $acquiredDateTime, $currVersion + 1, 0, $sizeOnDiskMB, $regionWeek);
ELSE
 UPDATE ${db.schema}."DATA" SET instanceUID = $instanceUID WHERE id = $dataId;
END_IF;
  END_IF;
 END_PROCEDURE;
create procedure ${db.schema}.${sp.name.prefix}UpdateGroup ("@id" BIGINT, $dataCount INT)
 AS
   UPDATE ${db.schema}."GROUP" SET dataCount = $dataCount WHERE id = "@id";
 END_PROCEDURE
