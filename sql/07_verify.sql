SELECT * FROM warehouse.vw_executive_kpis;
```

---

**Step 3 — Run it**

1. Click anywhere inside the query
2. Press `Cmd+Shift+E` to run it

---

**But first — you need to run SQL files 04, 05 and 06!**

The staging tables are loaded but the warehouse isn't populated yet. Do these in order first:

1. Open `04_load_dimensions.sql` → `Cmd+A` → `Cmd+Shift+E`
2. Open `05_load_facts.sql` → `Cmd+A` → `Cmd+Shift+E`
3. Open `06_create_views.sql` → `Cmd+A` → `Cmd+Shift+E`

Then run the verify query and you should see something like:
```
total_orders | total_revenue | avg_order_value | avg_delivery_days | avg_review_score
-------------|---------------|-----------------|-------------------|------------------
96,478       | 13,591,643.70 | 140.88          | 12.1              | 4.09