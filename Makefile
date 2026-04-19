# DataLink Local-First Medallion Pipeline — Makefile
# Phase 0 contains only the verify-phase-0 target. Full targets land in Phase 1.

.PHONY: help verify-phase-0

help:
	@echo "DataLink Pipeline — available targets:"
	@echo "  verify-phase-0   Verify Phase 0 deliverables exist and pass lint"
	@echo ""
	@echo "Phase 1+ targets (setup, up, down, demo, test, verify-phase-N) land in Phase 1."

# Phase 0: deliverables are docs/architecture.md + CLAUDE.md + .gitignore + .gitattributes.
# Verification: files exist, non-empty, and contain required section markers.
verify-phase-0:
	@echo "Verifying Phase 0 deliverables..."
	@test -s docs/architecture.md                 || { echo "FAIL: docs/architecture.md missing or empty"; exit 1; }
	@test -s CLAUDE.md                            || { echo "FAIL: CLAUDE.md missing or empty"; exit 1; }
	@test -s .gitignore                           || { echo "FAIL: .gitignore missing or empty"; exit 1; }
	@test -s .gitattributes                       || { echo "FAIL: .gitattributes missing or empty"; exit 1; }
	@grep -q "^## 3. Conflicts"   docs/architecture.md || { echo "FAIL: architecture.md missing Conflicts section"; exit 1; }
	@grep -q "^## 5. Local Stack" docs/architecture.md || { echo "FAIL: architecture.md missing Local Stack section"; exit 1; }
	@grep -q "^## 10. Open Questions" docs/architecture.md || { echo "FAIL: architecture.md missing Open Questions section"; exit 1; }
	@grep -q "^## 5. Plug-In" CLAUDE.md           || { echo "FAIL: CLAUDE.md missing Plug-In contract"; exit 1; }
	@grep -q "^## 8. Never Do" CLAUDE.md          || { echo "FAIL: CLAUDE.md missing Never Do list"; exit 1; }
	@echo "OK: Phase 0 deliverables present."
