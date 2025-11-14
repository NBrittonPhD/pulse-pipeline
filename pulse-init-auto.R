# ================================================================
#   PULSE PIPELINE AUTOMATED GIT INITIALIZATION SCRIPT
#   No prompts. No emojis. Pure R.
#   Run once from the repo root.
# ================================================================

# --------------------------
# 0. Set GitHub repo URL here
# --------------------------

repo_url <- "https://github.com/NBrittonPhD/pulse-pipeline.git"

# --------------------------
# 1. Check git installation
# --------------------------

git_ok <- system("git --version", intern = TRUE, ignore.stderr = TRUE)

if (length(git_ok) == 0) {
  stop("Git is not installed or not available on PATH.")
}

message("Git detected: ", git_ok)

# --------------------------
# 2. Initialize git repo (if needed)
# --------------------------

if (!dir.exists(".git")) {
  system("git init")
  message("Initialized new git repository.")
} else {
  message("Git repository already exists.")
}

# --------------------------
# 3. Create or reset main branch
# --------------------------

system("git checkout -B main")

# --------------------------
# 4. Add all files
# --------------------------

system("git add .")

# --------------------------
# 5. Commit initial scaffold
# --------------------------

system("git commit -m \"Initial PULSE pipeline scaffold\"")

message("Initial commit created.")

# --------------------------
# 6. Add or reset remote origin
# --------------------------

# Remove existing origin if present
system("git remote remove origin", ignore.stderr = TRUE, ignore.stdout = TRUE)

# Add new origin
system(paste("git remote add origin", shQuote(repo_url)))

message("Remote origin set to: ", repo_url)

# --------------------------
# 7. Push to GitHub
# --------------------------

message("Pushing to GitHub...")

system("git push -u origin main", ignore.stderr = FALSE, ignore.stdout = FALSE)

message("Automated git initialization complete.")
