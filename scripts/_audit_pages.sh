#!/usr/bin/env bash
# One-shot UI audit: read first 30 lines of every page in sidebar order.
cd /home/jatin/dev/DataPipelinesWithGX/datalink/ui
PAGES=(
    "control_tower.py"
    "pages/10_Pipeline_Control.py"
    "pages/14_Data_Model_Designer.py"
    "pages/13_Pipeline_Architect.py"
    "pages/12_Data_Contract_Architect.py"
    "pages/8_DQ_AI_Architect.py"
    "pages/1_DQ_Author.py"
    "pages/2_DQ_Review.py"
    "pages/6_DQ_Suite_Registry.py"
    "pages/9_Schema_Drift.py"
    "pages/11_Smart_Mapper.py"
    "pages/3_Executive_Dashboard.py"
    "pages/5_DQ_Dashboard.py"
    "pages/4_AI_Agents.py"
    "pages/7_Warehouse_Explorer.py"
)
for f in "${PAGES[@]}"; do
    echo "============================================================"
    echo " ${f}"
    echo "============================================================"
    head -30 "${f}" 2>&1
    echo ""
    # Count buttons + propose calls + history references
    btn=$(grep -c "st.button(" "${f}" 2>/dev/null || echo 0)
    propose=$(grep -ciE "propose|generate.*ai|ai.*generate|construct" "${f}" 2>/dev/null || echo 0)
    history=$(grep -ciE "history|audit_log|audit log|past propos" "${f}" 2>/dev/null || echo 0)
    echo "  >> stats: buttons=${btn}  propose-references=${propose}  history-references=${history}"
    echo ""
done
