# ================================================================
#   PULSE PIPELINE AUTOMATED GIT INITIALIZATION (FORCE VERSION)
#   Overwrites remote main branch. No prompts. No emojis.
# ================================================================

# --------------------------
# 0. Set GitHub repo URL here
# --------------------------

repo_url <- "https://github.com/NBrittonPhD/pulse-pipeline.git"

# --------------------------
# 1. Check for git installation
# --------------------------

git_ok <- system("git --version", intern = TRUE, ignore.stderr = TRUE)

if (length(git_ok) == 0) {
  stop("Git is not installed or not available on PATH.")
}

message("Git detected: ", git_ok)

# --------------------------
# 2. Initialize repo locally
# --------------------------

if (!dir.exists(".git")) {
  system("git init")
  message("Initialized new git repository.")
} else {
  message("Git repository already exists.")
}

# --------------------------
# 3. Ensure main branch exists and is active
# --------------------------

system("git checkout -B main")

# --------------------------
# 4. Stage everything
# --------------------------

system("git add .")

# --------------------------
# 5. Commit scaffold
# --------------------------

system("git commit -m \"Initial PULSE pipeline scaffold\"")

message("Initial commit created.")

# --------------------------
# 6. Configure remote
# --------------------------

# Remove existing remote if present
system("git remote remove origin", ignore.stderr = TRUE, ignore.stdout = TRUE)

# Add new remote
system(paste("git remote add origin", shQuote(repo_url)))

message("Remote origin set to: ", repo_url)

# --------------------------
# 7. Fo
