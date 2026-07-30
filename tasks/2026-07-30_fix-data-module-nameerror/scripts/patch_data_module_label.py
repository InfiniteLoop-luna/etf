from pathlib import Path

p = Path('/opt/etf-app/app.py')
s = p.read_text(encoding='utf-8')
old = "    favorite_module_label = get_module_label_for_page(FAVORITE_MY_FAVORITE_PAGE_LABEL)\n    money_module_label = get_module_label_for_page(MONEY_FLOW_PAGE_LABEL)\n    macro_module_label = get_module_label_for_page(MACRO_MAIN_PAGE_LABEL)\n"
new = "    favorite_module_label = get_module_label_for_page(FAVORITE_MY_FAVORITE_PAGE_LABEL)\n    money_module_label = get_module_label_for_page(MONEY_FLOW_PAGE_LABEL)\n    data_module_label = get_module_label_for_page(DATA_HEALTH_PAGE_LABEL)\n    macro_module_label = get_module_label_for_page(MACRO_MAIN_PAGE_LABEL)\n"
if old not in s:
    raise SystemExit('target block not found')
s = s.replace(old, new, 1)
p.write_text(s, encoding='utf-8')
