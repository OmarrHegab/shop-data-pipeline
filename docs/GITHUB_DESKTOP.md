# Publish using GitHub Desktop

## Apply this presentation pack

Copy `README.md`, `.gitignore`, `.gitattributes`, `docs/` and `scripts/` into your
existing `shop-data-pipeline` root, alongside all seven stage folders. This pack
contains presentation files and a Compose helper, not replacement pipeline code.
Merge ignore rules if a root `.gitignore` already exists. Do not replace your
stage folders: retain your local schedule and port changes.

## Add the existing folder

1. Open GitHub Desktop and sign in.
2. Choose **File > Add local repository**.
3. Select the existing `shop-data-pipeline` root folder.
4. If Desktop reports that this is not a Git repository, use its **create a
   repository here** link. Keep the repository at the existing root; do not
   create a second nested project. Do not generate another README or overwrite
   the supplied ignore rules.
5. If Desktop already knows the project, select it from **Current repository**.

## Review and commit

Review the Changes list before the first commit. It should include source code,
Compose files, Dockerfiles, requirements, documentation, and screenshots.
It should not include `data/`, `checkpoints/`, `logs/`, `.env`, ZIP downloads,
or stage 6's generated `config/jobs.json`.

Ignore rules do not untrack files committed earlier. If generated data or local
configuration already appears in History, pause publication and inspect that
history before proceeding. Do not delete working data to hide it from Git.

Suggested commit summary:

```text
Build end-to-end shop analytics pipeline with Kafka, Spark, Airflow and Streamlit
```

Click **Commit to main** (or your current branch).

## Publish

If this is a new local repository, click **Publish repository**.
If it already has the intended GitHub remote, use **Push origin**.
Do not replace or remove an existing remote just to follow this guide.

Suggested repository name:

```text
shop-data-pipeline
```

Suggested description:

```text
End-to-end shop analytics with Kafka, PySpark, Airflow, PostgreSQL and Streamlit. Dockerized Bronze/Silver/Gold pipeline with validation and scheduled runs.
```

For a public portfolio, deselect **Keep this code private** when publishing.
Publishing makes the selected repository content publicly accessible. The Compose
credentials are intentional local demo defaults, not personal account credentials.

On GitHub, use the About section's gear icon to add topics:

```text
data-engineering apache-kafka apache-spark pyspark apache-airflow
postgresql streamlit docker docker-compose etl medallion-architecture python
```

Do not put a localhost address in the repository Website field: it only works on
your own computer. The README screenshots show the working local interfaces.

Finally, open the repository on GitHub to check image paths and Mermaid diagrams.
Pin the project from your GitHub profile if you want it featured.

Official references:
- https://docs.github.com/en/desktop/adding-and-cloning-repositories/adding-a-repository-from-your-local-computer-to-github-desktop
- https://docs.github.com/en/desktop/adding-and-cloning-repositories/adding-an-existing-project-to-github-using-github-desktop
