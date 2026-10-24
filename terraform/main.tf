terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

locals {
  prefix = "${var.project}-${var.environment}"
  tags = {
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

data "aws_caller_identity" "current" {}

# ---------------------------------------------------------------- S3 data lake
resource "aws_s3_bucket" "lake" {
  bucket        = "${local.prefix}-lake-${data.aws_caller_identity.current.account_id}"
  force_destroy = true # lets `terraform destroy` clean up completely (dev only)
  tags          = local.tags
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Cost control: raw data moves to cheaper storage, then expires.
resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    id     = "raw-tiering"
    status = "Enabled"
    filter {
      prefix = "raw/"
    }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    expiration {
      days = 180
    }
  }
}

# ------------------------------------------------------- Glue catalog + Athena
resource "aws_glue_catalog_database" "this" {
  name = replace("${local.prefix}_db", "-", "_")
}

resource "aws_glue_catalog_table" "mart_city_daily" {
  name          = "mart_city_daily"
  database_name = aws_glue_catalog_database.this.name
  table_type    = "EXTERNAL_TABLE"
  parameters    = { classification = "parquet" }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.bucket}/curated/mart_city_daily.parquet"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }
    columns {
      name = "city_name"
      type = "string"
    }
    columns {
      name = "date"
      type = "date"
    }
    columns {
      name = "avg_pm25"
      type = "double"
    }
    columns {
      name = "max_pm25"
      type = "double"
    }
    columns {
      name = "avg_pm10"
      type = "double"
    }
    columns {
      name = "avg_no2"
      type = "double"
    }
    columns {
      name = "readings"
      type = "bigint"
    }
  }
}

resource "aws_athena_workgroup" "this" {
  name          = "${local.prefix}-wg"
  force_destroy = true
  configuration {
    enforce_workgroup_configuration = true
    bytes_scanned_cutoff_per_query  = 1073741824 # 1 GB guardrail against runaway cost
    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.bucket}/athena-results/"
    }
  }
  tags = local.tags
}

# -------------------------------------------------------- IAM (least privilege)
data "aws_iam_policy_document" "pipeline" {
  statement {
    sid       = "ReadWriteLake"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.lake.arn}/*"]
  }
  statement {
    sid       = "ListLake"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lake.arn]
  }
  statement {
    sid = "AthenaAndGlue"
    actions = ["athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults",
    "glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:GetPartitions"]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "pipeline" {
  name   = "${local.prefix}-pipeline"
  policy = data.aws_iam_policy_document.pipeline.json
}

data "aws_iam_policy_document" "assume_ec2" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "pipeline" {
  name               = "${local.prefix}-pipeline-role"
  assume_role_policy = data.aws_iam_policy_document.assume_ec2.json
  tags               = local.tags
}

resource "aws_iam_role_policy_attachment" "pipeline" {
  role       = aws_iam_role.pipeline.name
  policy_arn = aws_iam_policy.pipeline.arn
}

# ------------------------------------------------ Cost guardrail: budget alert
resource "aws_budgets_budget" "monthly" {
  name         = "${local.prefix}-budget"
  budget_type  = "COST"
  limit_amount = var.monthly_budget_usd
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }
}
