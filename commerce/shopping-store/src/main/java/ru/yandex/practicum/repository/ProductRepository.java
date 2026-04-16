package ru.yandex.practicum.repository;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.enums.ProductCategory;
import ru.yandex.practicum.enums.ProductStatus;
import ru.yandex.practicum.model.Product;

import java.util.List;

public interface ProductRepository extends JpaRepository<Product, Long> {
    List<Product> findByCategoryAndStatus(ProductCategory category, ProductStatus status);
    List<Product> findByStatus(ProductStatus status);

    Page<Product> findByCategoryAndStatus(ProductCategory category, ProductStatus status, Pageable pageable);
    Page<Product> findByStatus(ProductStatus status, Pageable pageable);
}