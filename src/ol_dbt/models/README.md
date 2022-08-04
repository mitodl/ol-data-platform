## dbt
Start here to learn more about dbt: https://docs.getdbt.com/docs/introduction

This project follows dbt guidelines pertaining to file organization. Efforts to follow these guidelines 
create logical project structure, making it easier to communicate and collaborate.

## Project Structure

- staging: initial connection to a data source, data prep (cleaning, renaming). Within this directory,
we organize files by product (e.g., mitxpro).

- intermediate: more complex transformations that typically bring data together. Within this directory,
we organize files by product (e.g., mitxpro).

- marts: often customer facing data that is organized to meet specific needs. 
