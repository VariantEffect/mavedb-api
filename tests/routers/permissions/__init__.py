"""The permission canary: no route gives a planted private record to a caller who may not see it (#810).

How it works. ``canary.py`` plants a small world of public and private records, each carrying a distinctive
marker string, and ``VISIBILITY`` says which personas may see each kind of record. ``test_canary.py`` calls every
swept route as every persona and fails if a response carries a marker that persona may not see, or if any persona
can't reach a marker it may see.

Three tripwires make the canary fail closed, so nothing new can skip it:

- ``test_route_registry.py``: every route the app serves, whatever its method, is listed in ``route_registry.py``
  as a ``Read``, a ``Write`` or an ``Exempt`` with a reason. Reads are swept; writes say what their response can
  carry and are swept once the test harness makes fresh data per persona cheap.
- ``test_personas.py``: every user role and collection role has a persona acting with it.
- Changing a permission rule fails the sweep or the positive control until ``VISIBILITY`` agrees with it.

To extend it:

- A new route: add it to ``ROUTES``. A read gets the requests to make, built from what ``plant_canary`` planted.
- A new role: plant a persona holding it, add it to ``PERSONAS`` and to ``ROLE_PERSONAS``.
- A new relationship that grants access, such as having created a record: no enum lists these, so plant a persona
  for it by hand and add it to ``PERSONAS``.
- A new kind of private record: plant one in ``plant_canary``, add its kind to ``VISIBILITY`` and its markers.

Not covered yet: no tripwire ties planted records to the guarded tables in ``mavedb.db.rls.RELATIONS``, so a new
guarded table goes unplanted silently until that check lands.
"""
