#!/bin/bash
# Backup original script
cp phase1_parallel_aws.py phase1_parallel_aws.py.original

# Create a modified version that uses ECR for skopeo only
sed -i 's|skopeo inspect docker://|skopeo inspect docker://public.ecr.aws/|g' phase1_parallel_aws.py
sed -i 's|skopeo inspect docker://public.ecr.aws/docker.io/library/|skopeo inspect docker://public.ecr.aws/|g' phase1_parallel_aws.py

# Run Phase 1 with your desired mode
python3 phase1_parallel_aws.py --smoke --workers 2

# Restore original
mv phase1_parallel_aws.py.original phase1_parallel_aws.py
