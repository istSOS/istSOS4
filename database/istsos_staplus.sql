-- Copyright 2025 SUPSI
-- 
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
-- 
--     https://www.apache.org/licenses/LICENSE-2.0
-- 
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

DO $BODY$
BEGIN
    IF current_setting('custom.staplus', true)::boolean THEN

        SET ROLE "administrator";

        /*--- STAPlus entity Party ---*/
        CREATE TABLE IF NOT EXISTS sensorthings."Party" (
        "id" BIGSERIAL PRIMARY KEY,
        "role" VARCHAR(255) NOT NULL,
        "description" TEXT,
        "displayName" VARCHAR(255),
        "authId" VARCHAR(255),          -- consider UNIQUE if you rely on it
        CONSTRAINT party_authId_unique UNIQUE ("authId")
        );

        ALTER TABLE sensorthings."Party"
        ADD COLUMN IF NOT EXISTS "role" VARCHAR(255);

        ALTER TABLE sensorthings."Party"
        ADD COLUMN IF NOT EXISTS "displayName" VARCHAR(255);

        ALTER TABLE sensorthings."Party"
        ADD COLUMN IF NOT EXISTS "description" TEXT;

        UPDATE sensorthings."Party"
        SET "role" = 'individual'
        WHERE "role" IS NULL;

        UPDATE sensorthings."Party"
        SET "role" = 'individual'
        WHERE "role" NOT IN ('individual', 'institutional');

        ALTER TABLE sensorthings."Party"
        ALTER COLUMN "role" SET NOT NULL,
        ALTER COLUMN "displayName" DROP NOT NULL,
        ALTER COLUMN "description" DROP NOT NULL;

        IF NOT EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conname = 'party_role_standard'
              AND conrelid = 'sensorthings."Party"'::regclass
        ) THEN
            ALTER TABLE sensorthings."Party"
            ADD CONSTRAINT party_role_standard
            CHECK ("role" IN ('individual', 'institutional'));
        END IF;

        CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."Party") RETURNS text AS $$
        SELECT '/Parties(' || $1.id || ')';
        $$ LANGUAGE SQL;

        /*--- Modifications to old STA entities ---*/
        ALTER TABLE sensorthings."Datastream"
        ADD COLUMN IF NOT EXISTS "party_id" BIGINT
        REFERENCES sensorthings."Party"(id)
        ON DELETE SET NULL;

        CREATE INDEX IF NOT EXISTS "idx_datastream_party_id"
        ON sensorthings."Datastream" USING btree ("party_id" ASC NULLS LAST);

        CREATE OR REPLACE FUNCTION "Datastreams@iot.navigationLink"(sensorthings."Party") RETURNS text AS $$
        SELECT '/Parties(' || $1.id || ')/Datastreams';
        $$ LANGUAGE SQL;

        CREATE OR REPLACE FUNCTION "Party@iot.navigationLink"(sensorthings."Datastream") RETURNS text AS $$
        SELECT CASE
            WHEN $1.party_id IS NOT NULL THEN '/Datastreams(' || $1.id || ')/Party'
            ELSE NULL
        END;
        $$ LANGUAGE SQL;


        ALTER TABLE sensorthings."Thing"
        ADD COLUMN IF NOT EXISTS "party_id" BIGINT
        REFERENCES sensorthings."Party"(id)
        ON DELETE SET NULL;

        CREATE INDEX IF NOT EXISTS "idx_thing_party_id"
        ON sensorthings."Thing" USING btree ("party_id" ASC NULLS LAST);

        CREATE OR REPLACE FUNCTION "Party@iot.navigationLink"(sensorthings."Thing") RETURNS text AS $$
        SELECT CASE
            WHEN $1.party_id IS NOT NULL THEN '/Things(' || $1.id || ')/Party'
            ELSE NULL
        END;
        $$ LANGUAGE SQL;

        CREATE OR REPLACE FUNCTION "Things@iot.navigationLink"(sensorthings."Party") RETURNS text AS $$
        SELECT '/Parties(' || $1.id || ')/Things';
        $$ LANGUAGE SQL;

        /*--- STAPlus entity License ---*/
        CREATE TABLE IF NOT EXISTS sensorthings."License" (
        "id" BIGSERIAL PRIMARY KEY,
        "name" VARCHAR(255) NOT NULL,
        "definition" TEXT NOT NULL,
        "description" TEXT,
        "logo" TEXT,
        "attributionText" TEXT
        );

        ALTER TABLE sensorthings."License"
        ADD COLUMN IF NOT EXISTS "definition" TEXT;

        ALTER TABLE sensorthings."License"
        ADD COLUMN IF NOT EXISTS "logo" TEXT;

        ALTER TABLE sensorthings."License"
        ADD COLUMN IF NOT EXISTS "attributionText" TEXT;

        ALTER TABLE sensorthings."License"
        ADD COLUMN IF NOT EXISTS "description" TEXT;

        UPDATE sensorthings."License"
        SET "definition" = ''
        WHERE "definition" IS NULL;

        ALTER TABLE sensorthings."License"
        ALTER COLUMN "definition" SET NOT NULL,
        ALTER COLUMN "description" DROP NOT NULL;

        CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."License") RETURNS text AS $$
        SELECT '/Licenses(' || $1.id || ')';
        $$ LANGUAGE SQL;

        /*--- Modifications to old STA entities ---*/
        ALTER TABLE sensorthings."Datastream"
        ADD COLUMN IF NOT EXISTS "license_id" BIGINT
        REFERENCES sensorthings."License"(id)
        ON DELETE SET NULL;

        CREATE INDEX IF NOT EXISTS "idx_datastream_license_id"
        ON sensorthings."Datastream" USING btree ("license_id" ASC NULLS LAST);

        CREATE OR REPLACE FUNCTION "Datastreams@iot.navigationLink"(sensorthings."License") RETURNS text AS $$
        SELECT '/Licenses(' || $1.id || ')/Datastreams';
        $$ LANGUAGE SQL;

        CREATE OR REPLACE FUNCTION "License@iot.navigationLink"(sensorthings."Datastream") RETURNS text AS $$
        SELECT CASE
            WHEN $1.license_id IS NOT NULL THEN '/Datastreams(' || $1.id || ')/License'
            ELSE NULL
        END;
        $$ LANGUAGE SQL;

        /*--- STAPlus entity Campaign ---*/
        CREATE TABLE IF NOT EXISTS sensorthings."Campaign" (
        "id" BIGSERIAL PRIMARY KEY,
        "name" VARCHAR(255) NOT NULL,
        "description" TEXT NOT NULL,
        "classification" VARCHAR(255),
        "termsOfUse" TEXT NOT NULL,
        "privacyPolicy" TEXT,
        "creationTime" TIMESTAMPTZ NOT NULL DEFAULT now(),
        "startTime" TIMESTAMPTZ,
        "endTime" TIMESTAMPTZ,
        "url" TEXT,

        "party_id" BIGINT
            REFERENCES sensorthings."Party"(id)
            ON DELETE SET NULL,

        "license_id" BIGINT
            REFERENCES sensorthings."License"(id)
            ON DELETE SET NULL
        );

        ALTER TABLE sensorthings."Campaign"
        ADD COLUMN IF NOT EXISTS "classification" VARCHAR(255);

        ALTER TABLE sensorthings."Campaign"
        ADD COLUMN IF NOT EXISTS "termsOfUse" TEXT;

        ALTER TABLE sensorthings."Campaign"
        ADD COLUMN IF NOT EXISTS "privacyPolicy" TEXT;

        ALTER TABLE sensorthings."Campaign"
        ADD COLUMN IF NOT EXISTS "creationTime" TIMESTAMPTZ DEFAULT now();

        ALTER TABLE sensorthings."Campaign"
        ADD COLUMN IF NOT EXISTS "startTime" TIMESTAMPTZ;

        ALTER TABLE sensorthings."Campaign"
        ADD COLUMN IF NOT EXISTS "endTime" TIMESTAMPTZ;

        ALTER TABLE sensorthings."Campaign"
        ADD COLUMN IF NOT EXISTS "url" TEXT;

        ALTER TABLE sensorthings."Campaign"
        DROP COLUMN IF EXISTS "legalBasis";

        UPDATE sensorthings."Campaign"
        SET "termsOfUse" = ''
        WHERE "termsOfUse" IS NULL;

        UPDATE sensorthings."Campaign"
        SET "creationTime" = now()
        WHERE "creationTime" IS NULL;

        ALTER TABLE sensorthings."Campaign"
        ALTER COLUMN "termsOfUse" SET NOT NULL,
        ALTER COLUMN "creationTime" SET NOT NULL;

        CREATE INDEX IF NOT EXISTS "idx_campaign_party_id"
        ON sensorthings."Campaign" USING btree ("party_id" ASC NULLS LAST);

        CREATE INDEX IF NOT EXISTS "idx_campaign_license_id"
        ON sensorthings."Campaign" USING btree ("license_id" ASC NULLS LAST);

        CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."Campaign") RETURNS text AS $$
        SELECT '/Campaigns(' || $1.id || ')';
        $$ LANGUAGE SQL;

        CREATE OR REPLACE FUNCTION "Party@iot.navigationLink"(sensorthings."Campaign") RETURNS text AS $$
        SELECT CASE
            WHEN $1.party_id IS NOT NULL THEN '/Campaigns(' || $1.id || ')/Party'
            ELSE NULL
        END;
        $$ LANGUAGE SQL;

        CREATE OR REPLACE FUNCTION "Campaigns@iot.navigationLink"(sensorthings."Party") RETURNS text AS $$
        SELECT '/Parties(' || $1.id || ')/Campaigns';
        $$ LANGUAGE SQL;

        CREATE OR REPLACE FUNCTION "License@iot.navigationLink"(sensorthings."Campaign") RETURNS text AS $$
        SELECT CASE
            WHEN $1.license_id IS NOT NULL THEN '/Campaigns(' || $1.id || ')/License'
            ELSE NULL
        END;
        $$ LANGUAGE SQL;

        CREATE OR REPLACE FUNCTION "Campaigns@iot.navigationLink"(sensorthings."License") RETURNS text AS $$
        SELECT '/Licenses(' || $1.id || ')/Campaigns';
        $$ LANGUAGE SQL;

        CREATE TABLE IF NOT EXISTS sensorthings."Campaign_Datastream" (
        "campaign_id" BIGINT NOT NULL
            REFERENCES sensorthings."Campaign"(id) ON DELETE CASCADE,

        "datastream_id" BIGINT NOT NULL
            REFERENCES sensorthings."Datastream"(id) ON DELETE CASCADE,

        CONSTRAINT campaign_datastream_unique UNIQUE ("campaign_id", "datastream_id")
        );

        CREATE INDEX IF NOT EXISTS "idx_campaign_datastream_campaign_id"
        ON sensorthings."Campaign_Datastream" USING btree ("campaign_id" ASC NULLS LAST);

        CREATE INDEX IF NOT EXISTS "idx_campaign_datastream_datastream_id"
        ON sensorthings."Campaign_Datastream" USING btree ("datastream_id" ASC NULLS LAST);

        CREATE OR REPLACE FUNCTION "Datastreams@iot.navigationLink"(sensorthings."Campaign") RETURNS text AS $$
        SELECT '/Campaigns(' || $1.id || ')/Datastreams';
        $$ LANGUAGE SQL;

        CREATE OR REPLACE FUNCTION "Campaigns@iot.navigationLink"(sensorthings."Datastream") RETURNS text AS $$
        SELECT '/Datastreams(' || $1.id || ')/Campaigns';
        $$ LANGUAGE SQL;

        /*--- STAPlus entity ObservationGroup ---*/
        CREATE TABLE IF NOT EXISTS sensorthings."ObservationGroup" (
        "id" BIGSERIAL PRIMARY KEY,
        "name" VARCHAR(255) NOT NULL,
        "description" TEXT NOT NULL,
        "purpose" TEXT,
        "creationTime" TIMESTAMPTZ NOT NULL DEFAULT now(),
        "endTime" TIMESTAMPTZ,
        "termsOfUse" TEXT,
        "privacyPolicy" TEXT,
        "dataQuality" jsonb DEFAULT NULL,
        "properties" jsonb DEFAULT NULL,

        "party_id" BIGINT
            REFERENCES sensorthings."Party"(id)
            ON DELETE SET NULL,

        "license_id" BIGINT
            REFERENCES sensorthings."License"(id)
            ON DELETE SET NULL
        );

        ALTER TABLE sensorthings."ObservationGroup"
        ADD COLUMN IF NOT EXISTS "purpose" TEXT;

        ALTER TABLE sensorthings."ObservationGroup"
        ADD COLUMN IF NOT EXISTS "creationTime" TIMESTAMPTZ DEFAULT now();

        ALTER TABLE sensorthings."ObservationGroup"
        ADD COLUMN IF NOT EXISTS "endTime" TIMESTAMPTZ;

        ALTER TABLE sensorthings."ObservationGroup"
        ADD COLUMN IF NOT EXISTS "termsOfUse" TEXT;

        ALTER TABLE sensorthings."ObservationGroup"
        ADD COLUMN IF NOT EXISTS "privacyPolicy" TEXT;

        ALTER TABLE sensorthings."ObservationGroup"
        ADD COLUMN IF NOT EXISTS "dataQuality" jsonb DEFAULT NULL;

        IF EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'sensorthings'
              AND table_name = 'ObservationGroup'
              AND column_name = 'dataQuality'
              AND data_type <> 'jsonb'
        ) THEN
            EXECUTE 'ALTER TABLE sensorthings."ObservationGroup"
                     ALTER COLUMN "dataQuality" TYPE jsonb
                     USING CASE
                         WHEN "dataQuality" IS NULL THEN NULL
                         ELSE to_jsonb("dataQuality")
                     END';
        END IF;

        UPDATE sensorthings."ObservationGroup"
        SET "creationTime" = now()
        WHERE "creationTime" IS NULL;

        ALTER TABLE sensorthings."ObservationGroup"
        ALTER COLUMN "creationTime" SET NOT NULL;

        CREATE INDEX IF NOT EXISTS "idx_observationgroup_party_id"
        ON sensorthings."ObservationGroup" USING btree ("party_id" ASC NULLS LAST);

        CREATE INDEX IF NOT EXISTS "idx_observationgroup_license_id"
        ON sensorthings."ObservationGroup" USING btree ("license_id" ASC NULLS LAST);

        /* selfLink */
        CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."ObservationGroup") RETURNS text AS $$
        SELECT '/ObservationGroups(' || $1.id || ')';
        $$ LANGUAGE SQL;

        /* ObservationGroup -> Campaigns/Party/License navlinks */
        DROP FUNCTION IF EXISTS "Campaign@iot.navigationLink"(sensorthings."ObservationGroup");

        CREATE OR REPLACE FUNCTION "Campaigns@iot.navigationLink"(sensorthings."ObservationGroup") RETURNS text AS $$
        SELECT '/ObservationGroups(' || $1.id || ')/Campaigns';
        $$ LANGUAGE SQL;

        CREATE OR REPLACE FUNCTION "Party@iot.navigationLink"(sensorthings."ObservationGroup") RETURNS text AS $$
        SELECT CASE
            WHEN $1.party_id IS NOT NULL THEN '/ObservationGroups(' || $1.id || ')/Party'
            ELSE NULL
        END;
        $$ LANGUAGE SQL;

        CREATE OR REPLACE FUNCTION "License@iot.navigationLink"(sensorthings."ObservationGroup") RETURNS text AS $$
        SELECT CASE
            WHEN $1.license_id IS NOT NULL THEN '/ObservationGroups(' || $1.id || ')/License'
            ELSE NULL
        END;
        $$ LANGUAGE SQL;

        CREATE TABLE IF NOT EXISTS sensorthings."Campaign_ObservationGroup" (
        "campaign_id" BIGINT NOT NULL
            REFERENCES sensorthings."Campaign"(id) ON DELETE CASCADE,

        "observationgroup_id" BIGINT NOT NULL
            REFERENCES sensorthings."ObservationGroup"(id) ON DELETE CASCADE,

        CONSTRAINT campaign_observationgroup_unique UNIQUE ("campaign_id", "observationgroup_id")
        );

        IF EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'sensorthings'
              AND table_name = 'ObservationGroup'
              AND column_name = 'campaign_id'
        ) THEN
            EXECUTE 'INSERT INTO sensorthings."Campaign_ObservationGroup"
                     ("campaign_id", "observationgroup_id")
                     SELECT "campaign_id", id
                     FROM sensorthings."ObservationGroup"
                     WHERE "campaign_id" IS NOT NULL
                     ON CONFLICT ("campaign_id", "observationgroup_id") DO NOTHING';

            ALTER TABLE sensorthings."ObservationGroup"
            DROP COLUMN IF EXISTS "campaign_id";
        END IF;

        CREATE INDEX IF NOT EXISTS "idx_campaign_observationgroup_campaign_id"
        ON sensorthings."Campaign_ObservationGroup" USING btree ("campaign_id" ASC NULLS LAST);

        CREATE INDEX IF NOT EXISTS "idx_campaign_observationgroup_observationgroup_id"
        ON sensorthings."Campaign_ObservationGroup" USING btree ("observationgroup_id" ASC NULLS LAST);

        /* Campaign/Party/License -> ObservationGroups navlinks */
        CREATE OR REPLACE FUNCTION "ObservationGroups@iot.navigationLink"(sensorthings."Campaign") RETURNS text AS $$
        SELECT '/Campaigns(' || $1.id || ')/ObservationGroups';
        $$ LANGUAGE SQL;

        CREATE OR REPLACE FUNCTION "ObservationGroups@iot.navigationLink"(sensorthings."Party") RETURNS text AS $$
        SELECT '/Parties(' || $1.id || ')/ObservationGroups';
        $$ LANGUAGE SQL;

        CREATE OR REPLACE FUNCTION "ObservationGroups@iot.navigationLink"(sensorthings."License") RETURNS text AS $$
        SELECT '/Licenses(' || $1.id || ')/ObservationGroups';
        $$ LANGUAGE SQL;

        CREATE TABLE IF NOT EXISTS sensorthings."ObservationGroup_Observation" (
        "observationgroup_id" BIGINT NOT NULL
            REFERENCES sensorthings."ObservationGroup"(id) ON DELETE CASCADE,

        "observation_id" BIGINT NOT NULL
            REFERENCES sensorthings."Observation"(id, "phenomenonTime") ON DELETE CASCADE,

        CONSTRAINT observationgroup_observation_unique UNIQUE ("observationgroup_id", "observation_id")
        );

        CREATE INDEX IF NOT EXISTS "idx_observationgroup_observation_group_id"
        ON sensorthings."ObservationGroup_Observation" USING btree ("observationgroup_id" ASC NULLS LAST);

        CREATE INDEX IF NOT EXISTS "idx_observationgroup_observation_observation_id"
        ON sensorthings."ObservationGroup_Observation" USING btree ("observation_id" ASC NULLS LAST);

        /* ObservationGroup -> Observations */
        CREATE OR REPLACE FUNCTION "Observations@iot.navigationLink"(sensorthings."ObservationGroup") RETURNS text AS $$
        SELECT '/ObservationGroups(' || $1.id || ')/Observations';
        $$ LANGUAGE SQL;

        /* Observation -> ObservationGroups */
        CREATE OR REPLACE FUNCTION "ObservationGroups@iot.navigationLink"(sensorthings."Observation") RETURNS text AS $$
        SELECT '/Observations(' || $1.id || ')/ObservationGroups';
        $$ LANGUAGE SQL;

        /* Observation -> Relations where this Observation is the Subject/Object */
        CREATE OR REPLACE FUNCTION "Objects@iot.navigationLink"(sensorthings."Observation") RETURNS text AS $$
        SELECT '/Observations(' || $1.id || ')/Objects';
        $$ LANGUAGE SQL;

        CREATE OR REPLACE FUNCTION "Subjects@iot.navigationLink"(sensorthings."Observation") RETURNS text AS $$
        SELECT '/Observations(' || $1.id || ')/Subjects';
        $$ LANGUAGE SQL;

        /*--- STAPlus entity Relation ---*/
        CREATE TABLE IF NOT EXISTS sensorthings."Relation" (
        "id" BIGSERIAL PRIMARY KEY,

        "role" VARCHAR(255) NOT NULL,
        "description" TEXT,
        "properties" jsonb DEFAULT NULL,

        "subject_id" BIGINT NOT NULL
            REFERENCES sensorthings."Observation"(id, "phenomenonTime")
            ON DELETE CASCADE,

        "object_id" BIGINT
            REFERENCES sensorthings."Observation"(id, "phenomenonTime")
            ON DELETE SET NULL,

        "externalResource" TEXT
        );

        ALTER TABLE sensorthings."Relation"
        ADD COLUMN IF NOT EXISTS "role" VARCHAR(255);

        ALTER TABLE sensorthings."Relation"
        ADD COLUMN IF NOT EXISTS "description" TEXT;

        IF EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'sensorthings'
              AND table_name = 'Relation'
              AND column_name = 'type'
        ) THEN
            EXECUTE 'UPDATE sensorthings."Relation"
                     SET "role" = COALESCE("role", "type")
                     WHERE "role" IS NULL';
        END IF;

        UPDATE sensorthings."Relation"
        SET "role" = 'relatedTo'
        WHERE "role" IS NULL;

        ALTER TABLE sensorthings."Relation"
        ALTER COLUMN "role" SET NOT NULL;

        ALTER TABLE sensorthings."Relation"
        DROP COLUMN IF EXISTS "type";

        /* Enforce XOR: either object_id OR externalResource must be set (but not both) */
        IF NOT EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conname = 'relation_object_xor_external'
              AND conrelid = 'sensorthings."Relation"'::regclass
        ) THEN
            ALTER TABLE sensorthings."Relation"
            ADD CONSTRAINT relation_object_xor_external
            CHECK (
                (object_id IS NOT NULL AND "externalResource" IS NULL)
                OR
                (object_id IS NULL AND "externalResource" IS NOT NULL)
            );
        END IF;

        CREATE INDEX IF NOT EXISTS "idx_relation_subject_id"
        ON sensorthings."Relation" USING btree ("subject_id" ASC NULLS LAST);

        CREATE INDEX IF NOT EXISTS "idx_relation_object_id"
        ON sensorthings."Relation" USING btree ("object_id" ASC NULLS LAST);

        /* selfLink */
        CREATE OR REPLACE FUNCTION "@iot.selfLink"(sensorthings."Relation") RETURNS text AS $$
        SELECT '/Relations(' || $1.id || ')';
        $$ LANGUAGE SQL;

        CREATE TABLE IF NOT EXISTS sensorthings."Relation_ObservationGroup" (
        "relation_id" BIGINT NOT NULL
            REFERENCES sensorthings."Relation"(id) ON DELETE CASCADE,

        "observationgroup_id" BIGINT NOT NULL
            REFERENCES sensorthings."ObservationGroup"(id) ON DELETE CASCADE,

        CONSTRAINT relation_observationgroup_unique UNIQUE ("relation_id", "observationgroup_id")
        );

        IF EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'sensorthings'
              AND table_name = 'Relation'
              AND column_name = 'observationgroup_id'
        ) THEN
            EXECUTE 'INSERT INTO sensorthings."Relation_ObservationGroup"
                     ("relation_id", "observationgroup_id")
                     SELECT id, "observationgroup_id"
                     FROM sensorthings."Relation"
                     WHERE "observationgroup_id" IS NOT NULL
                     ON CONFLICT ("relation_id", "observationgroup_id") DO NOTHING';

            ALTER TABLE sensorthings."Relation"
            DROP COLUMN IF EXISTS "observationgroup_id";
        END IF;

        CREATE INDEX IF NOT EXISTS "idx_relation_observationgroup_relation_id"
        ON sensorthings."Relation_ObservationGroup" USING btree ("relation_id" ASC NULLS LAST);

        CREATE INDEX IF NOT EXISTS "idx_relation_observationgroup_observationgroup_id"
        ON sensorthings."Relation_ObservationGroup" USING btree ("observationgroup_id" ASC NULLS LAST);

        /* Relation -> ObservationGroups */
        DROP FUNCTION IF EXISTS "ObservationGroup@iot.navigationLink"(sensorthings."Relation");

        CREATE OR REPLACE FUNCTION "ObservationGroups@iot.navigationLink"(sensorthings."Relation") RETURNS text AS $$
        SELECT '/Relations(' || $1.id || ')/ObservationGroups';
        $$ LANGUAGE SQL;

        /* Relation -> Subject */
        CREATE OR REPLACE FUNCTION "Subject@iot.navigationLink"(sensorthings."Relation") RETURNS text AS $$
        SELECT '/Relations(' || $1.id || ')/Subject';
        $$ LANGUAGE SQL;

        /* Relation -> Object (only if object_id exists) */
        CREATE OR REPLACE FUNCTION "Object@iot.navigationLink"(sensorthings."Relation") RETURNS text AS $$
        SELECT CASE
            WHEN $1.object_id IS NOT NULL THEN '/Relations(' || $1.id || ')/Object'
            ELSE NULL
        END;
        $$ LANGUAGE SQL;

        /* ObservationGroup -> Relations */
        CREATE OR REPLACE FUNCTION "Relations@iot.navigationLink"(sensorthings."ObservationGroup") RETURNS text AS $$
        SELECT '/ObservationGroups(' || $1.id || ')/Relations';
        $$ LANGUAGE SQL;

        IF COALESCE(current_setting('custom.authorization', true)::boolean, false)
            OR COALESCE(current_setting('custom.versioning', true)::boolean, false) THEN

            ALTER TABLE sensorthings."Party"
            ADD COLUMN IF NOT EXISTS "commit_id" BIGINT
            REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE;

            ALTER TABLE sensorthings."License"
            ADD COLUMN IF NOT EXISTS "commit_id" BIGINT
            REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE;

            ALTER TABLE sensorthings."Campaign"
            ADD COLUMN IF NOT EXISTS "commit_id" BIGINT
            REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE;

            ALTER TABLE sensorthings."ObservationGroup"
            ADD COLUMN IF NOT EXISTS "commit_id" BIGINT
            REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE;

            ALTER TABLE sensorthings."Relation"
            ADD COLUMN IF NOT EXISTS "commit_id" BIGINT
            REFERENCES sensorthings."Commit"(id) ON DELETE CASCADE;

            CREATE INDEX IF NOT EXISTS "idx_party_commit_id"
            ON sensorthings."Party" USING btree ("commit_id" ASC NULLS LAST);

            CREATE INDEX IF NOT EXISTS "idx_license_commit_id"
            ON sensorthings."License" USING btree ("commit_id" ASC NULLS LAST);

            CREATE INDEX IF NOT EXISTS "idx_campaign_commit_id"
            ON sensorthings."Campaign" USING btree ("commit_id" ASC NULLS LAST);

            CREATE INDEX IF NOT EXISTS "idx_observationgroup_commit_id"
            ON sensorthings."ObservationGroup" USING btree ("commit_id" ASC NULLS LAST);

            CREATE INDEX IF NOT EXISTS "idx_relation_commit_id"
            ON sensorthings."Relation" USING btree ("commit_id" ASC NULLS LAST);

            CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."Party") RETURNS text AS $$
            SELECT CASE
                WHEN $1.commit_id IS NOT NULL THEN '/Parties(' || $1.id || ')/Commit(' || $1.commit_id || ')'
                ELSE NULL
            END;
            $$ LANGUAGE SQL;

            CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."License") RETURNS text AS $$
            SELECT CASE
                WHEN $1.commit_id IS NOT NULL THEN '/Licenses(' || $1.id || ')/Commit(' || $1.commit_id || ')'
                ELSE NULL
            END;
            $$ LANGUAGE SQL;

            CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."Campaign") RETURNS text AS $$
            SELECT CASE
                WHEN $1.commit_id IS NOT NULL THEN '/Campaigns(' || $1.id || ')/Commit(' || $1.commit_id || ')'
                ELSE NULL
            END;
            $$ LANGUAGE SQL;

            CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."ObservationGroup") RETURNS text AS $$
            SELECT CASE
                WHEN $1.commit_id IS NOT NULL THEN '/ObservationGroups(' || $1.id || ')/Commit(' || $1.commit_id || ')'
                ELSE NULL
            END;
            $$ LANGUAGE SQL;

            CREATE OR REPLACE FUNCTION "Commit@iot.navigationLink"(sensorthings."Relation") RETURNS text AS $$
            SELECT CASE
                WHEN $1.commit_id IS NOT NULL THEN '/Relations(' || $1.id || ')/Commit(' || $1.commit_id || ')'
                ELSE NULL
            END;
            $$ LANGUAGE SQL;

            CREATE OR REPLACE FUNCTION "Parties@iot.navigationLink"(sensorthings."Commit") RETURNS text AS $$
            SELECT CASE
                WHEN EXISTS (SELECT 1 FROM sensorthings."Party" WHERE commit_id = $1.id)
                THEN '/Commits(' || $1.id || ')/Parties'
                ELSE NULL
            END;
            $$ LANGUAGE SQL;

            CREATE OR REPLACE FUNCTION "Licenses@iot.navigationLink"(sensorthings."Commit") RETURNS text AS $$
            SELECT CASE
                WHEN EXISTS (SELECT 1 FROM sensorthings."License" WHERE commit_id = $1.id)
                THEN '/Commits(' || $1.id || ')/Licenses'
                ELSE NULL
            END;
            $$ LANGUAGE SQL;

            CREATE OR REPLACE FUNCTION "Campaigns@iot.navigationLink"(sensorthings."Commit") RETURNS text AS $$
            SELECT CASE
                WHEN EXISTS (SELECT 1 FROM sensorthings."Campaign" WHERE commit_id = $1.id)
                THEN '/Commits(' || $1.id || ')/Campaigns'
                ELSE NULL
            END;
            $$ LANGUAGE SQL;

            CREATE OR REPLACE FUNCTION "ObservationGroups@iot.navigationLink"(sensorthings."Commit") RETURNS text AS $$
            SELECT CASE
                WHEN EXISTS (SELECT 1 FROM sensorthings."ObservationGroup" WHERE commit_id = $1.id)
                THEN '/Commits(' || $1.id || ')/ObservationGroups'
                ELSE NULL
            END;
            $$ LANGUAGE SQL;

            CREATE OR REPLACE FUNCTION "Relations@iot.navigationLink"(sensorthings."Commit") RETURNS text AS $$
            SELECT CASE
                WHEN EXISTS (SELECT 1 FROM sensorthings."Relation" WHERE commit_id = $1.id)
                THEN '/Commits(' || $1.id || ')/Relations'
                ELSE NULL
            END;
            $$ LANGUAGE SQL;

        END IF;

        RESET ROLE;
    END IF;
END $BODY$;
