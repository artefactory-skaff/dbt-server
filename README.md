# CI/CD

## Build the package

From the root of the project:

```poetry build```

## To run integration tests in the CI

Integration tests require authentication to GCP. To this end, you need to set up the Workload Identity Federation

Create pool:
```gcloud iam workload-identity-pools create my-pool --project="stc-dbt-test-9e19" --location="global" --display-name="Demo pool"```

Create provider:
```gcloud iam workload-identity-pools providers create-oidc "my-provider" --project="stc-dbt-test-9e19" \```
  ```--location="global" \ ```
  ```--workload-identity-pool="my-pool" \ ```
  ```--display-name="Demo provider" \ ```
  ```--attribute-mapping="google.subject=assertion.sub,attribute.actor=assertion.actor,attribute.aud=assertion.aud,attribute.repository=assertion.repository" \ ```
  ```--issuer-uri="https://token.actions.githubusercontent.com"```

Link pool to repo:
```gcloud iam service-accounts add-iam-policy-binding "stc-dbt-sa@stc-dbt-test-9e19.iam.gserviceaccount.com" \ ```
  ```--project="stc-dbt-test-9e19" \ ```
  ```--role="roles/iam.serviceAccountTokenCreator" \ ```
  ```--member="principalSet://iam.googleapis.com/projects/956787288/locations/global/workloadIdentityPools/my-pool/attribute.repository/artefactory-fr/dbt-server"```

permissions to set:
- roles/iam.serviceAccountTokenCreator
- iam.serviceAccountUser
- roles/iam.workloadIdentityUser
