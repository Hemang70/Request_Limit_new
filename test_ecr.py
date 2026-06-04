import subprocess

image = "nginx:alpine"
ecr_image = "public.ecr.aws/nginx/nginx:alpine"

# Test skopeo with ECR
result = subprocess.run(
    ["skopeo", "inspect", f"docker://{ecr_image}"],
    capture_output=True, text=True
)
print(f"ECR skopeo: {result.returncode}")
print(result.stdout[:200] if result.returncode == 0 else result.stderr[:200])

# Test docker pull with original (uses Docker Hub)
result2 = subprocess.run(
    ["docker", "pull", image],
    capture_output=True, text=True
)
print(f"Docker pull: {result2.returncode}")
