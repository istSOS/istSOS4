# Copyright 2025 SUPSI
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Pydantic schemas for POST /Register (restricted-access registration).

Design decisions
----------------
* ``ContactInfo`` is kept as a separate nested model so it serialises
  cleanly to a JSONB column via ``.model_dump()``.  Merging the flat
  ``explanation`` string into that dict at the DB layer (rather than here)
  keeps the model layer pure and transport-agnostic.

* All ``ContactInfo`` fields are ``Optional[str]`` (default ``None``) so
  a submitter need only supply the contact details they have available.

* ``RestrictedRegistrationRequest`` deliberately does *not* inherit any
  auth-aware base class — this endpoint is intentionally public (no
  ``Depends(get_current_user)``).  The pending role assigned in the DB
  ensures the new account has zero privileges until an admin approves it.
"""

from typing import Optional

from pydantic import BaseModel


class ContactInfo(BaseModel):
    """Optional structured contact details for a restricted-access applicant.

    All fields are optional; the applicant provides whatever is relevant.
    The entire model is stored as a single JSONB blob in
    ``sensorthings."User".contact``.
    """

    domain: Optional[str] = None
    company: Optional[str] = None
    address: Optional[str] = None
    telephone: Optional[str] = None
    telegram: Optional[str] = None
    linkedin: Optional[str] = None


class RestrictedRegistrationRequest(BaseModel):
    """Request body for POST /Register.

    Fields
    ------
    username:       Desired login handle.  Uniqueness enforced at the DB level.
    password:       Plain-text password; hashed with bcrypt before storage.
    dataset_id:     Human-readable or URI identifier for the STAC dataset the
                    applicant wants access to.
    odrl_policy_id: Identifier of the ODRL policy document that governs access
                    to the requested dataset.
    explanation:    Free-text justification for the access request.  Stored
                    inside the ``contact`` JSONB blob alongside ContactInfo.
    contact_info:   Structured contact details for the applicant.
    """

    username: str
    password: str
    dataset_id: str
    odrl_policy_id: str
    explanation: str
    contact_info: ContactInfo
