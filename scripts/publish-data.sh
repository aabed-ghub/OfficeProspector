#!/usr/bin/env bash
# Uploads the local firms.json to a GitHub Release and triggers a Pages redeploy.
# Usage: ./scripts/publish-data.sh [tag]
# Requires: gh CLI authenticated (https://cli.github.com)

set -euo pipefail

TAG="${1:-latest-data}"
FILE="docs/site/firms.json"

if [ ! -f "$FILE" ]; then
  echo "Error: $FILE not found. Run the pipeline first: python -m src.cli run"
  exit 1
fi

SIZE=$(du -h "$FILE" | cut -f1)
FIRMS=$(python3 -c "import json; print(json.load(open('$FILE'))['totalFirms'])")
echo "Publishing $FILE ($SIZE, $FIRMS firms) as release '$TAG'..."

# Delete existing release if updating
gh release delete "$TAG" --yes 2>/dev/null || true
git tag -d "$TAG" 2>/dev/null || true
git push origin ":refs/tags/$TAG" 2>/dev/null || true

# Create release with firms.json as an asset
gh release create "$TAG" "$FILE" \
  --title "Pipeline Data ($TAG)" \
  --notes "Dashboard data: $FIRMS firms, generated $(date -I)."

echo "Release created. Triggering Pages redeploy..."
gh workflow run deploy-pages.yml 2>/dev/null || echo "Note: Trigger the Pages deploy manually if needed."

echo "Done! Dashboard will update at https://aabed-ghub.github.io/OfficeProspector/"
