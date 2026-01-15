-- =====================================================================
-- Artefact Challenge - Création du schéma dédié
-- Fichier 01_schema.sql
-- =====================================================================

-- Création d'un schéma clair et dédié (très bonne pratique)
CREATE SCHEMA IF NOT EXISTS sales;

-- 01_create_schema.sql
-- Création du schéma dédié + configuration de base

CREATE SCHEMA IF NOT EXISTS sales;

-- Optionnel mais apprécié : on positionne le search_path par défaut
ALTER DATABASE ecommerce SET search_path TO sales, public;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

COMMENT ON SCHEMA sales IS 'Schéma principal contenant les données normalisées du challenge Artefact e-commerce';

-- Message de confirmation
SELECT 'Schéma sales créé ou déjà existant' AS status;

