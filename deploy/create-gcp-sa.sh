#!/usr/bin/env bash

set -e
set -u

copy_to_clipboard() {
    local file="$1"
    case "$(uname -s)" in
        Linux)
            # Linux generally uses xclip or xsel. xclip is used here.
            if command -v xclip >/dev/null 2>&1; then
                xclip -selection clipboard < "$file"
            else
                echo "xclip is not installed. Please install it to use this function."
                return 1
            fi
            ;;
        Darwin)
            # macOS uses pbcopy
            pbcopy < "$file"
            ;;
        CYGWIN*|MINGW32*|MSYS*|MINGW*)
            # Windows with Git Bash or similar environment uses /dev/clipboard
            cat "$file" > /dev/clipboard
            ;;
        *)
            echo "Unsupported operating system."
            return 1
            ;;
    esac
}

export PROJECT_ID=${PROJECT_ID:-$(gcloud config get-value project)}
echo "Working in project $PROJECT_ID"

export SA_NAME=${SA_NAME:-"substratus-control-plane"}
set -x

# Create a service account if it doesn't exist
if gcloud iam service-accounts list | grep -q $SA_NAME; then
    echo "Service account $SA_NAME already exists"
else
    gcloud iam service-accounts create substratus-control-plane --display-name "Service account used by Substratus Control Plane to manage AI infrastructure"
fi

# Assign privleges to the service account
# Assign compute admin
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member serviceAccount:$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com \
    --role roles/compute.admin

# Create a keyfile if it doesn't exist
keyfile_path="/tmp/substratus-sa.keyfile.json"
if [ -f $keyfile_path ]; then
    echo "Keyfile $keyfile_path already exists"
else
    gcloud iam service-accounts keys create $keyfile_path \
        --iam-account $SA_NAME@$PROJECT_ID.iam.gserviceaccount.com
fi

echo "Your service account keyfile is at $keyfile_path."
echo "The contents have been copied to your clipboard."
copy_to_clipboard "$keyfile_path"

