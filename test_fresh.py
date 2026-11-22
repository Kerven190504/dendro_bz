import requests, collections
tests = [("Дуб обыкновенный", "Quercus robur"), ("Клён остролистный", "Acer platanoides"),
         ("Берёза повислая", "Betula pendula"), ("Сосна обыкновенная", "Pinus sylvestris")]
all_ms = []
for name, lat in tests:
    d = requests.get("http://localhost:8000/api/fresh",
                     params={"subject": lat, "name": name, "refresh": 1}, timeout=90).json()
    print(name, dict(collections.Counter(i["kind"] for i in d["items"])))
    for s in d["sources"]:
        all_ms.append(s["ms"])
        print("  ", s["source"], s["ms"], "мс,", s["count"], "шт.", "" if s["ok"] else s["detail"])
print("Время ответа: от", min(all_ms), "до", max(all_ms), "мс, в среднем", sum(all_ms)//len(all_ms), "мс")