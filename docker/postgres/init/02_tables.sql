-- =====================================================================
-- Artefact CI Challenge - Schéma 3FN - ecommerce_sales
-- Date: Janvier 2026
-- =====================================================================


SET search_path TO sales, public;

-- Suppression dans l'ordre inverse des dépendances (pour les tests locaux)
DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS customers CASCADE;
DROP TABLE IF EXISTS dim_dates CASCADE;

-- ---------------------------------------------------------------------------
-- 1. Clients
-- ---------------------------------------------------------------------------
CREATE TABLE sales.customers (
    customer_id     BIGINT          PRIMARY KEY,
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    email           VARCHAR(255)    UNIQUE,
    gender          VARCHAR(20)     CHECK (gender IN ('M', 'F', 'Other', 'Unknown', NULL)),
    age_range       VARCHAR(20),
    signup_date     DATE,
    country         VARCHAR(100),
    created_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------------------------
-- 2. Produits (catalogue)
-- ---------------------------------------------------------------------------
CREATE TABLE sales.products (
    product_id      INTEGER         PRIMARY KEY,
    product_name    VARCHAR(255)    NOT NULL,
    category        VARCHAR(100),
    brand           VARCHAR(100),
    color           VARCHAR(50),
    size            VARCHAR(50),
    catalog_price   DECIMAL(12,2)   NOT NULL CHECK (catalog_price >= 0),
    cost_price      DECIMAL(12,2)   CHECK (cost_price >= 0 AND cost_price <= catalog_price),
    created_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------------------------
-- 3. Dimension date
-- ---------------------------------------------------------------------------
CREATE TABLE sales.dim_dates (
    date_id         INTEGER     PRIMARY KEY,         -- YYYYMMDD
    full_date       DATE        NOT NULL UNIQUE,
    year            SMALLINT    NOT NULL,
    month           SMALLINT    NOT NULL,
    day             SMALLINT    NOT NULL,
    day_of_week     VARCHAR(10),
    week_number     SMALLINT,
    quarter         SMALLINT,
    is_weekend      BOOLEAN,
    is_holiday      BOOLEAN     DEFAULT FALSE,
    created_at      TIMESTAMP   DEFAULT CURRENT_TIMESTAMP
);


-- ---------------------------------------------------------------------------
-- 4. En-tête des commandes
-- ---------------------------------------------------------------------------
CREATE TABLE sales.orders (
    sale_id             BIGINT          PRIMARY KEY,
    sale_date           DATE            NOT NULL,
    date_id             INTEGER         NOT NULL REFERENCES sales.dim_dates(date_id),
    customer_id         BIGINT          NOT NULL REFERENCES sales.customers(customer_id) ON DELETE RESTRICT,
    channel             VARCHAR(50),
    channel_campaigns   VARCHAR(100),
    created_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------------------------
-- 5. Lignes de commande (table de faits principale)
-- ---------------------------------------------------------------------------
CREATE TABLE sales.order_items (
    item_id             BIGINT          PRIMARY KEY,
    sale_id             BIGINT          NOT NULL
        REFERENCES sales.orders(sale_id) ON DELETE CASCADE,
    product_id          INTEGER         NOT NULL
        REFERENCES sales.products(product_id) ON DELETE RESTRICT,

    quantity            INTEGER         NOT NULL CHECK (quantity > 0),
    original_price      DECIMAL(12,2)   NOT NULL CHECK (original_price >= 0),
    unit_price          DECIMAL(12,2)   NOT NULL CHECK (unit_price >= 0),
    discount_applied    DECIMAL(12,2)   DEFAULT 0 CHECK (discount_applied >= 0),
    discount_percent    DECIMAL(5,2)    DEFAULT 0 CHECK (discount_percent BETWEEN 0 AND 100),
    discounted          BOOLEAN         DEFAULT FALSE,
    item_total          DECIMAL(12,2)   GENERATED ALWAYS AS (quantity * unit_price) STORED,
                        CHECK (item_total >= 0),

    created_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    -- Contraintes métier
    CONSTRAINT chk_discount_consistency CHECK (
        (discount_applied = 0 AND discount_percent = 0) OR
        (discount_applied > 0 OR discount_percent > 0)
    )
);

-- ---------------------------------------------------------------------------
-- Index
-- ---------------------------------------------------------------------------
CREATE INDEX idx_orders_date        ON sales.orders(sale_date);
CREATE INDEX idx_orders_customer    ON sales.orders(customer_id);
CREATE INDEX idx_order_items_sale   ON sales.order_items(sale_id);
CREATE INDEX idx_order_items_product ON sales.order_items(product_id);
CREATE INDEX idx_order_items_date   ON sales.order_items USING btree ((sale_id::text || '_' || product_id));

-- ---------------------------------------------------------------------------
-- Vues
-- ---------------------------------------------------------------------------
CREATE VIEW sales.orders_with_total AS
SELECT
    o.*,
    COALESCE(SUM(oi.item_total), 0) AS total_amount
FROM sales.orders o
LEFT JOIN sales.order_items oi ON oi.sale_id = o.sale_id
GROUP BY o.sale_id;

-- ---------------------------------------------------------------------------
-- Commentaires
-- ---------------------------------------------------------------------------
COMMENT ON SCHEMA sales IS 'Schéma contenant toutes les données du challenge Artefact e-commerce';
COMMENT ON TABLE sales.customers     IS 'Référentiel clients uniques';
COMMENT ON TABLE sales.products      IS 'Référentiel produits (catalogue)';
COMMENT ON TABLE sales.orders        IS 'En-tête des commandes / ventes';
COMMENT ON TABLE sales.order_items   IS 'Détail des lignes de vente (table de faits)';
COMMENT ON TABLE sales.dim_dates     IS 'Dimension temps pour analyses temporelles';