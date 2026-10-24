output "lake_bucket" {
  value = aws_s3_bucket.lake.bucket
}

output "athena_workgroup" {
  value = aws_athena_workgroup.this.name
}

output "glue_database" {
  value = aws_glue_catalog_database.this.name
}

output "pipeline_role_arn" {
  value = aws_iam_role.pipeline.arn
}
