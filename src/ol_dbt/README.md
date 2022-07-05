### Notes

This project is configured to use Trino as the warehouse engine. The profile information
is defined in the repository with environment variables for the username and
password. There are separate profiles defined for QA and Production environments.

To run a dbt build you can use the command:
```
DBT_TRINO_USERNAME=<your_username> DBT_TRINO_PASSWORD=<your_password> dbt build --project-dir src/ol_dbt/ --profiles-dir src/ol_dbt/ --target <qa|production>
```

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
