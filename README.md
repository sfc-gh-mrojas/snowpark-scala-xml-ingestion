# Snowpark Scala project reading XML

This project demonstrates how to read XML data from Snowflake using Snowpark Scala.

## Setup

Set the following environment variables with your Snowflake account information

```bash
# Linux/MacOS
set SNOWSQL_ACCOUNT=<replace with your account identifer>
set SNOWSQL_USER=<replace with your username>
set SNOWSQL_PWD=<replace with your password>
set SNOWSQL_DATABASE=<replace with your database>
set SNOWSQL_SCHEMA=<replace with your schema>
set SNOWSQL_WAREHOUSE=<replace with your warehouse>
```

```powershell
# Windows/PowerShell
$env:SNOWSQL_ACCOUNT = "<replace with your account identifer>"
$env:SNOWSQL_USER = "<replace with your username>"
$env:SNOWSQL_PWD = "<replace with your password>"
$env:SNOWSQL_DATABASE = "<replace with your database>"
$env:SNOWSQL_SCHEMA = "<replace with your schema>"
$env:SNOWSQL_WAREHOUSE = "<replace with your warehouse>"
```

Optional: You can set this env var permanently by editing your bash profile (on Linux/MacOS) or 
using the System Properties menu (on Windows).

> If you use VSCODE, you can set these environment variables using an .env file in your workspace folder. You can the set this file in the launch.json file in the .vscode folder. 


## Prereqs

To develop your applications locally, you will need:

- A Snowflake account
- [Java 11](https://adoptium.net/temurin/releases/?version=11)
- [Scala 2.12](https://www.scala-lang.org/download/)
- An IDE or code editor (IntelliJ, VS Code, Eclipse)

Before getting started to run your application the expectation is that you create the `EXTRACT_XML` function before running the application.