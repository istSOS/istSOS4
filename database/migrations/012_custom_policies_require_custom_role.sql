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

-- =============================================================================
-- Migration: 012_custom_policies_require_custom_role
-- Description: Per-user policies written through POST /Policies
--              (permissions.type = "custom") were gated only on
--                  current_app_user_id() = ANY (<ids>) AND (<condition>)
--              so they kept applying after an administrator moved the user
--              to another role. Policies are permissive and OR together,
--              which let a user demoted to viewer keep reading -- and
--              writing -- rows outside their new role and Network scope.
--
--              api/app/v1/endpoints/create/policy.py now adds
--                  current_app_user_role() = 'custom'
--              to every new custom policy. This migration adds the same
--              check to custom policies that already exist. They stay in
--              place and apply again if the user is moved back to custom.
--
--              Custom policies are recognised by their identity clause
--              (current_app_user_id() = ANY ...), which no built-in rbac_*
--              policy uses. Policies that already carry the role check are
--              skipped, so the migration is idempotent.
-- =============================================================================

DO $BODY$
DECLARE
    p       record;
    role_ok constant text := '(sensorthings.current_app_user_role() = ''custom''::text)';
BEGIN
    IF current_setting('custom.authorization', true)::boolean THEN

        SET ROLE "administrator";

        FOR p IN
            SELECT tablename, policyname, cmd, qual, with_check
            FROM pg_policies
            WHERE schemaname = 'sensorthings'
              AND policyname NOT LIKE 'rbac\_%'
              AND (coalesce(qual, '') || coalesce(with_check, ''))
                  LIKE '%current_app_user_id() = ANY%'
              AND (coalesce(qual, '') || coalesce(with_check, ''))
                  NOT LIKE '%current_app_user_role() = ''custom''%'
        LOOP
            IF p.qual IS NOT NULL AND p.with_check IS NOT NULL THEN
                EXECUTE format(
                    'ALTER POLICY %I ON sensorthings.%I USING (%s AND %s) WITH CHECK (%s AND %s);',
                    p.policyname, p.tablename, role_ok, p.qual, role_ok, p.with_check
                );
            ELSIF p.qual IS NOT NULL THEN
                EXECUTE format(
                    'ALTER POLICY %I ON sensorthings.%I USING (%s AND %s);',
                    p.policyname, p.tablename, role_ok, p.qual
                );
            ELSE
                EXECUTE format(
                    'ALTER POLICY %I ON sensorthings.%I WITH CHECK (%s AND %s);',
                    p.policyname, p.tablename, role_ok, p.with_check
                );
            END IF;
            RAISE NOTICE 'Migration 012: added the custom role check to policy %', p.policyname;
        END LOOP;

        RESET ROLE;

    END IF;
END $BODY$;
