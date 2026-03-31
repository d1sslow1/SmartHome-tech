package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.dto.ProductCategory;
import ru.yandex.practicum.dto.ProductState;
import ru.yandex.practicum.model.Product;

import java.util.List;
import java.util.UUID;

@Repository
public interface ProductRepository extends JpaRepository<Product, UUID> {

    List<Product> findByState(ProductState state);

    List<Product> findByCategoryAndState(ProductCategory category, ProductState state);

    List<Product> findByCategory(ProductCategory category);
}