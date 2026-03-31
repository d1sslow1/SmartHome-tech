-- Таблица для товаров на складе
CREATE TABLE IF NOT EXISTS warehouse_products (
                                                  product_id UUID PRIMARY KEY,
                                                  quantity INTEGER NOT NULL DEFAULT 0,
                                                  width DOUBLE PRECISION NOT NULL,
                                                  height DOUBLE PRECISION NOT NULL,
                                                  depth DOUBLE PRECISION NOT NULL,
                                                  weight DOUBLE PRECISION NOT NULL,
                                                  fragile BOOLEAN NOT NULL DEFAULT FALSE
);

-- Индекс для быстрого поиска
CREATE INDEX IF NOT EXISTS idx_warehouse_products_quantity ON warehouse_products(quantity);