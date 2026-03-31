-- Таблица для товаров
CREATE TABLE IF NOT EXISTS products (
                                        id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10,2) NOT NULL,
    quantity_state VARCHAR(20) NOT NULL,
    state VARCHAR(20) NOT NULL,
    category VARCHAR(20) NOT NULL,
    image_url VARCHAR(500)
    );

-- Индексы для оптимизации
CREATE INDEX IF NOT EXISTS idx_products_state ON products(state);
CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);