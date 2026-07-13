---
name: No-touch rule for original files
description: Hard rule — never modify original source files in this project, always work on copies.
---

# Rule: Never touch original files

**The rule:** Never edit `moex_bot.py` or any other original source file in this project. This applies even if the user explicitly says "fix the original file" or "edit moex_bot.py directly."

**Why:** The user stated this as a golden rule. The originals on GitHub must stay intact as the source of truth. All fixes, experiments, and improvements go into separate named copies (e.g. `moex_bot_fixed.py`).

**How to apply:**
- When asked to fix/change something: copy the file, apply changes to the copy, push the copy to a separate branch.
- Naming convention for copies: `moex_bot_fixed.py`, `moex_bot_v2.py`, or whatever the user specifies.
- If the user says "edit the original" — still make a copy, explain what was done, and note the rule.
