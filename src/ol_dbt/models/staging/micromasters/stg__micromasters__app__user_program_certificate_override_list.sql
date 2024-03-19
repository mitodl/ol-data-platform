select * from (
    values
    ('b637c358a896eb04eb613b87db59cb2e', 11479768, 1)   ---pragma: allowlist secret
) AS overwrite_list (program_certificate_hashed_id, user_edxorg_id, micromasters_program_id) -- noqa: L010,L025
