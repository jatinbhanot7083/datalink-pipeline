"""Post-Validation Crew — runs AFTER a GX checkpoint failure to:
1. Root-cause the failure
2. Suggest a remediation plan (NEVER auto-execute)
3. Report findings to Ops (Email + Teams webhook stub)
"""
