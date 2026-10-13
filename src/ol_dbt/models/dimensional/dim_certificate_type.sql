select 1 as certificate_type_pk, 'verified' as certificate_type_code, 'Verified Certificate' as certificate_type_name, true as certificate_requires_id_verification
union all
select 2 as certificate_type_pk, 'professional' as certificate_type_code, 'Professional Certificate' as certificate_type_name, true as certificate_requires_id_verification
union all
select 3 as certificate_type_pk, 'completion' as certificate_type_code, 'Certificate of Completion' as certificate_type_name, false as certificate_requires_id_verification
union all
select 4 as certificate_type_pk, 'audit' as certificate_type_code, 'Audit Certificate' as certificate_type_name, false as certificate_requires_id_verification
union all
select 5 as certificate_type_pk, 'micromasters' as certificate_type_code, 'MicroMasters Credential' as certificate_type_name, true as certificate_requires_id_verification
union all
select 6 as certificate_type_pk, 'honor' as certificate_type_code, 'Honor Certificate' as certificate_type_name, false as certificate_requires_id_verification
union all
select 7 as certificate_type_pk, 'credit' as certificate_type_code, 'Credit Certificate' as certificate_type_name, true as certificate_requires_id_verification
