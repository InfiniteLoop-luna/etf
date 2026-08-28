from src.stock_subsector_classifier import classify_stock_subsector


def test_classifies_pcb_and_mlcc_from_product_text():
    pcb = classify_stock_subsector(industry="元器件", product="高多层印制电路板及PCB产品")
    mlcc = classify_stock_subsector(industry="元器件", main_business="片式多层陶瓷电容器（MLCC）的研发生产")

    assert pcb["subsector"] == "PCB"
    assert "PCB" in pcb["subsector_tags"]
    assert mlcc["subsector"] == "MLCC"


def test_classifies_semiconductor_equipment_into_smaller_segments():
    cmp_item = classify_stock_subsector(industry="半导体", introduction="主营CMP设备和化学机械抛光工艺")
    etch = classify_stock_subsector(industry="半导体", introduction="等离子体刻蚀设备和薄膜沉积设备")
    deposition = classify_stock_subsector(industry="半导体", main_business="PECVD、ALD薄膜沉积设备")

    assert cmp_item["subsector"] == "CMP设备及材料"
    assert etch["subsector"] == "刻蚀设备"
    assert "薄膜沉积设备" in etch["subsector_tags"]
    assert deposition["subsector"] == "薄膜沉积设备"


def test_falls_back_to_original_industry_without_guessing():
    result = classify_stock_subsector(industry="食品饮料", main_business="产品暂无细分描述")

    assert result["subsector"] == "其他·食品饮料"
    assert result["subsector_tags"] == []
