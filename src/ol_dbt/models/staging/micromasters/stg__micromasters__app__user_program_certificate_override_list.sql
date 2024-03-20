select * from (
    values
    ('74869f589baf0522ec4341ee3b7970526b6de2918bc6bf29873eae040d6c2359', 11479768, 1)   ---pragma: allowlist secret
) AS overwrite_list (program_certificate_hashed_id, user_edxorg_id, micromasters_program_id) -- noqa: L010,L025
