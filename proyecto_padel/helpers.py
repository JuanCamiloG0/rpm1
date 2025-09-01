# helpers.py
from types import SimpleNamespace

CATS_M = ['7ma', '6ta', '5ta', '4ta', '3ra',
          '2_3', '2da', '1ra']          # top → abajo
CATS_F = ['Femenino 1ra', 'Femenino A', 'Femenino B', 'Femenino C', 'Femenino D',
          'Femenino E']                  # top → abajo


def assign_buckets(rows, cats, genero=None, rank_key='rpm', descending=True, max_per_cat=10):
    """
    rows: lista de dicts (p.ej. traídos de Sheets) con al menos:
          nombre, rpm (o el campo que uses), genero ('M'/'F'), _movement opcional
    cats: orden de categorías de mayor a menor (ej. CATS_M o CATS_F)
    genero: None (general), 'M' o 'F'
    rank_key: campo para ordenar (ej. 'rpm' o '_rank')
    """
    # filtrar por género si aplica
    data = [r for r in rows if (genero is None or r.get('genero') == genero)]

    # orden global de ranking
    data.sort(key=lambda r: r.get(rank_key, 0), reverse=descending)

    groups = {c: [] for c in cats}
    overall_pos = 0
    cat_i = 0

    for r in data:
        overall_pos += 1
        # si la categoría actual está llena, avanza
        while cat_i < len(cats) and len(groups[cats[cat_i]]) >= max_per_cat:
            cat_i += 1
        # si no hay más categorías, mete en la última (o podrías cortar)
        if cat_i >= len(cats):
            cat_i = len(cats) - 1

        cat = cats[cat_i]
        # posición dentro de la categoría
        pos_in_cat = len(groups[cat]) + 1

        # guarda campos útiles para la plantilla
        r_assigned = dict(r)
        r_assigned['_pos'] = overall_pos
        r_assigned['cat'] = cat
        r_assigned['_pos_cat'] = pos_in_cat

        groups[cat].append(r_assigned)

    # categoría “top” por defecto (la primera que tenga jugadores)
    default_cat = next((c for c in cats if groups[c]), cats[0])
    return groups, default_cat
