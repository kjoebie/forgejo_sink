# Automatische Log Tabel Initialisatie

## Probleem

Voorheen moesten de log tabellen (`logs.bronze_processing_log`, `logs.bronze_run_summary`, etc.) handmatig aangemaakt worden voordat de notebooks konden draaien. Als een error optrad in `process_bronze_layer` voordat de log tabellen bestonden, kon de error niet gelogd worden en crashte de notebook met een misleidende "table not found" error in plaats van de echte foutmelding.

## Oplossing

Vanaf nu worden **alle log tabellen automatisch aangemaakt** wanneer `log_batch()` of `log_summary()` voor het eerst wordt aangeroepen. Dit betekent:

✅ **Errors worden altijd gelogd**, zelfs als de log tabellen nog niet bestaan
✅ **Geen handmatige setup meer nodig** - tabellen worden automatisch aangemaakt
✅ **Idempotent** - functies kunnen veilig meerdere keren aangeroepen worden
✅ **Zero breaking changes** - bestaande code blijft gewoon werken

## Wat is er Veranderd?

### 1. Nieuwe Functie: `ensure_log_tables()`

In `modules/logging_utils.py` is een nieuwe functie toegevoegd:

```python
from modules.logging_utils import ensure_log_tables

# Maak alle log tabellen aan (indien nodig)
ensure_log_tables(spark, debug=True)
```

**Features:**
- Maakt de `logs` schema aan (indien niet bestaat)
- Maakt alle 4 log tabellen aan met correcte schemas en partitioning:
  - `logs.bronze_processing_log` (partitioned by `run_date`, `table_name`)
  - `logs.bronze_run_summary`
  - `logs.silver_processing_log` (partitioned by `run_date`)
  - `logs.silver_run_summary`
- Idempotent: kan veilig meerdere keren aangeroepen worden
- Gebruikt internal caching om performance overhead te minimaliseren

### 2. Automatische Initialisatie in Log Functies

`log_batch()` en `log_summary()` roepen nu automatisch `ensure_log_tables()` aan:

```python
# Voorheen: zou crashen als log tabellen niet bestaan
log_batch(spark, bronze_results, "bronze", run_log_id="abc123")

# Nu: maakt automatisch tabellen aan indien nodig
log_batch(spark, bronze_results, "bronze", run_log_id="abc123")  # ✅ Werkt altijd!
```

### 3. Setup Notebook (Optioneel)

Voor één-keer initialisatie is er nu een dedicated notebook:

**`notebooks/01_setup_log_tables.ipynb`**

Dit notebook:
- Roept `ensure_log_tables()` aan met debug logging
- Verifieert dat alle tabellen succesvol zijn aangemaakt
- Toont de table schemas
- Optioneel: test de log functies met dummy data

**Let op:** Dit notebook is **optioneel** - de log tabellen worden automatisch aangemaakt bij het eerste gebruik.

## Gebruik

### Optie 1: Automatisch (Aanbevolen)

Doe niets! De log tabellen worden automatisch aangemaakt wanneer nodig:

```python
from modules.logging_utils import log_summary, log_batch

# Eerste keer: maakt tabellen automatisch aan
run_log_id = log_summary(spark, bronze_summary, layer="bronze")
log_batch(spark, bronze_results, layer="bronze", run_log_id=run_log_id)
```

### Optie 2: Expliciet Setup Notebook

Run het setup notebook één keer om de tabellen expliciet aan te maken:

```bash
# In Fabric
Run notebook: notebooks/01_setup_log_tables.ipynb

# In Cluster met Papermill
papermill notebooks/01_setup_log_tables.ipynb output.ipynb
```

### Optie 3: Programmatisch

Roep `ensure_log_tables()` expliciet aan in je code:

```python
from modules.logging_utils import ensure_log_tables

# Maak tabellen aan (indien nodig)
ensure_log_tables(spark, debug=True)

# Nu kun je log functies gebruiken
log_batch(spark, records, "bronze", run_log_id="...")
```

## Error Scenario Test

Het originele probleem is nu opgelost:

```python
# SCENARIO: process_bronze_layer faalt, log tabellen bestaan niet

try:
    result = process_bronze_table(
        spark, table_def, source, run_id, run_ts, run_date
    )
except Exception as e:
    # Voorheen: deze log call zou crashen met "table not found"
    # Nu: maakt automatisch tabellen aan en logt de error ✅
    error_record = {
        "status": "FAILED",
        "error_message": str(e),
        # ... other fields
    }
    log_batch(spark, [error_record], "bronze", run_log_id=run_log_id)
```

## Performance

De auto-initialisatie heeft **minimale performance impact**:

1. **Eerste call**: Check + creatie (~1-2 seconden voor alle 4 tabellen)
2. **Volgende calls**: Gecached, geen overhead (0ms)

De `_log_tables_initialized` flag zorgt ervoor dat `ensure_log_tables()` maar één keer per Spark sessie uitgevoerd wordt.

## Backwards Compatibility

✅ **100% backwards compatible** - alle bestaande code blijft werken
✅ Tabellen die al bestaan worden niet overschreven
✅ Geen breaking changes in functie signatures
✅ Bestaande notebooks werken zonder aanpassingen

## Testing

Run de unit tests om de functionaliteit te verifiëren:

```bash
# Run log initialization tests
pytest tests/test_log_table_initialization.py -v

# Run alle tests
pytest tests/ -v
```

De test suite bevat:
- Schema creation test
- Table creation test
- Idempotency test
- Partitioning verification
- Auto-creation in log_batch() test
- Auto-creation in log_summary() test
- **Error logging without existing tables test** (het originele probleem scenario!)

## Bestanden Gewijzigd

### Modified
- `modules/logging_utils.py`
  - Toegevoegd: `ensure_log_tables()` functie (regel 152-260)
  - Toegevoegd: `_log_tables_initialized` flag (regel 149)
  - Gewijzigd: `log_batch()` - roept nu `ensure_log_tables()` aan (regel 580)
  - Gewijzigd: `log_summary()` - roept nu `ensure_log_tables()` aan (regel 651)

### Created
- `notebooks/01_setup_log_tables.ipynb` - Setup notebook voor één-keer initialisatie
- `tests/test_log_table_initialization.py` - Unit tests voor log table initialisatie
- `LOG_TABLE_AUTO_INITIALIZATION.md` - Deze documentatie

## Migration Guide

### Voor Nieuwe Projecten
Geen actie nodig - alles werkt automatisch!

### Voor Bestaande Projecten

**Optie A: Doe niets**
De bestaande tabellen blijven werken. Auto-creatie wordt alleen gebruikt als tabellen niet bestaan.

**Optie B: Run setup notebook**
Voor duidelijkheid kun je het setup notebook één keer runnen:
```bash
papermill notebooks/01_setup_log_tables.ipynb output.ipynb
```

**Optie C: Test eerst**
Run de unit tests om te verifiëren dat alles werkt:
```bash
pytest tests/test_log_table_initialization.py -v
```

## Veelgestelde Vragen

**Q: Moet ik mijn bestaande notebooks aanpassen?**
A: Nee, alle bestaande code blijft werken zonder aanpassingen.

**Q: Worden mijn bestaande log tabellen overschreven?**
A: Nee, `ensure_log_tables()` checkt eerst of tabellen bestaan en laat ze met rust als ze al bestaan.

**Q: Wat als ik de tabellen handmatig wil aanmaken?**
A: Dat kan nog steeds! Run het `01_setup_log_tables.ipynb` notebook of roep `ensure_log_tables()` expliciet aan.

**Q: Werkt dit in zowel Fabric als Cluster omgevingen?**
A: Ja, de code werkt in beide omgevingen. Environment detectie gebeurt automatisch via `path_utils.py`.

**Q: Wat als table creatie faalt?**
A: De functie raist een exception met een duidelijke error message. Check de logs voor details.

**Q: Hoe weet ik of de tabellen succesvol zijn aangemaakt?**
A: Check de logs - `ensure_log_tables()` logt "✓ Created {table}" voor elke nieuwe tabel. Of run het setup notebook met `debug=True`.

## Conclusie

Dit lost het oorspronkelijke probleem op: **Errors worden nu ALTIJD gelogd**, zelfs als de log tabellen nog niet bestaan. De notebooks crashen niet meer met misleidende "table not found" errors wanneer `process_bronze_layer` faalt.

De oplossing is backwards compatible, heeft minimale performance overhead, en vereist geen code aanpassingen in bestaande notebooks.
