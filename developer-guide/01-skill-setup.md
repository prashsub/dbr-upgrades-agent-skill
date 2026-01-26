# Step 1: Setting Up the DBR Migration Skill

This guide walks you through installing the Databricks LTS Migration skill in your workspace.

## Prerequisites

- [ ] Databricks workspace access
- [ ] Agent mode enabled in Databricks Assistant
- [ ] Access to the `databricks-dbr-migration` skill folder

---

## Step 1.1: Locate Your Skills Folder

Skills live in your user workspace at:

```
/Users/{your-username}/.assistant/skills/
```

### Option A: Via Databricks Assistant UI

1. Open the **Databricks Assistant** panel (click the Assistant icon in the sidebar)
2. Click the **⚙️ Settings** (gear icon)
3. Click **↗️ Open skills folder**

### Option B: Via Workspace Browser

1. Navigate to **Workspace** in the left sidebar
2. Go to `/Users/{your-username}/`
3. Create `.assistant/skills/` folder if it doesn't exist

---

## Step 1.2: Copy the Migration Skill

Copy the entire `databricks-dbr-migration` folder to your skills directory.

### Final Structure

```
/Users/{your-username}/.assistant/skills/
└── databricks-dbr-migration/
    ├── SKILL.md                              # Main skill definition
    ├── references/
    │   ├── BREAKING-CHANGES.md               # Complete breaking changes
    │   ├── SPARK-CONNECT-GUIDE.md            # Spark Connect patterns
    │   ├── MIGRATION-CHECKLIST.md            # Migration checklist
    │   └── SCALA-213-GUIDE.md                # Scala 2.13 guide
    ├── scripts/
    │   ├── scan-breaking-changes.py          # Code scanner
    │   ├── apply-fixes.py                    # Auto-fix script
    │   └── validate-migration.py             # Validation script
    └── assets/
        ├── fix-patterns.json                 # Fix patterns
        ├── migration-template.sql            # SQL config
        └── version-matrix.json               # Version data
```

---

## Step 1.3: Verify Installation

### Test 1: Check Skill Recognition

1. Open a notebook in your workspace
2. Open **Databricks Assistant** (agent mode)
3. Ask: `What skills do you have for DBR migration?`

**Expected Response:** The Assistant should mention the `databricks-dbr-migration` skill.

### Test 2: Test Skill Activation

Ask the Assistant:
```
Can you help me scan my notebooks for DBR 17.3 compatibility issues?
```

**Expected Response:** The Assistant should load the skill and offer to scan your code.

---

## Step 1.4: Configure Skill for Your Team (Optional)

For team-wide deployment, you can place the skill in a shared location:

### Option A: Shared Workspace Folder

1. Create a shared skills folder: `/Shared/.assistant/skills/`
2. Copy the skill there
3. Each team member symlinks to their personal skills folder

### Option B: Git Integration

1. Store the skill in a Git repository
2. Clone to each developer's workspace
3. Symlink to `.assistant/skills/`

---

## Troubleshooting

### Skill Not Recognized

**Symptom:** Assistant doesn't mention the skill when asked.

**Solutions:**
1. Verify the folder structure is correct
2. Check that `SKILL.md` exists in the skill folder
3. Ensure the `name:` field in SKILL.md frontmatter is valid
4. Try refreshing the Assistant (close and reopen)

### Skill Loads But Doesn't Work

**Symptom:** Assistant mentions the skill but can't use it.

**Solutions:**
1. Check file permissions in the skill folder
2. Verify Python scripts have correct syntax
3. Review the `allowed-tools` field in SKILL.md

### Agent Mode Not Available

**Symptom:** Can't enable agent mode in Assistant.

**Solutions:**
1. Check with your workspace admin - agent mode may need to be enabled
2. Verify your workspace is on a supported plan
3. Contact Databricks support

---

## Next Steps

Once the skill is installed:

→ **[02-using-assistant.md](02-using-assistant.md)**: Learn how to use the Assistant to scan and fix your code

---

## References

- [Extend Assistant with Agent Skills - Microsoft Learn](https://learn.microsoft.com/en-us/azure/databricks/assistant/skills)
- [Agent Skills Specification](https://agentskills.io/specification)
