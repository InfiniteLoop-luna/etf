from pathlib import Path

p = Path(r"D:\sourcecode\etf\app.py")
s = p.read_text(encoding="utf-8")

s = s.replace(
    "elif mobile_page == MONEY_HOTMONEY_PAGE_LABEL:\n                render_hotmoney_tab()\n            elif mobile_page == MONEY_FRESHNESS_PAGE_LABEL:\n                render_funding_freshness_page()\n            else:\n                render_moneyflow_tab()\n",
    "elif mobile_page == MONEY_HOTMONEY_PAGE_LABEL:\n                render_hotmoney_tab()\n            else:\n                render_moneyflow_tab()\n",
)

s = s.replace(
    "    money_module_label = get_module_by_id(\"money\").label\n    macro_module_label = get_module_by_id(\"macro\").label\n    decision_module_label = get_module_by_id(\"decision\").label\n",
    "    money_module_label = get_module_by_id(\"money\").label\n    data_module_label = get_module_by_id(\"data\").label\n    macro_module_label = get_module_by_id(\"macro\").label\n    decision_module_label = get_module_by_id(\"decision\").label\n",
)

s = s.replace(
    "        elif selected_page == MONEY_HOTMONEY_PAGE_LABEL:\n            render_hotmoney_tab()\n        elif selected_page == MONEY_FRESHNESS_PAGE_LABEL:\n            render_funding_freshness_page()\n        else:\n            render_moneyflow_tab()\n\n    elif selected_module == macro_module_label:\n",
    "        elif selected_page == MONEY_HOTMONEY_PAGE_LABEL:\n            render_hotmoney_tab()\n        else:\n            render_moneyflow_tab()\n\n    elif selected_module == data_module_label:\n        render_funding_freshness_page()\n\n    elif selected_module == macro_module_label:\n",
)

p.write_text(s, encoding="utf-8")
