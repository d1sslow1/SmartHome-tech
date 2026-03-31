-- Таблица корзин
CREATE TABLE IF NOT EXISTS carts (
                                     id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    username VARCHAR(255) NOT NULL UNIQUE,
    state VARCHAR(20) NOT NULL DEFAULT 'ACTIVE'
    );

-- Таблица товаров в корзине
CREATE TABLE IF NOT EXISTS cart_items (
                                          id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    cart_id UUID NOT NULL REFERENCES carts(id) ON DELETE CASCADE,
    product_id UUID NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 1
    );

-- Индексы для оптимизации
CREATE INDEX IF NOT EXISTS idx_carts_username ON carts(username);
CREATE INDEX IF NOT EXISTS idx_cart_items_cart_id ON cart_items(cart_id);
CREATE INDEX IF NOT EXISTS idx_cart_items_product_id ON cart_items(product_id);