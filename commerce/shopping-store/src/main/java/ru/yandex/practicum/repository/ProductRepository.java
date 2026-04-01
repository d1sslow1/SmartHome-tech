package ru.yandex.practicum.repository;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.dto.ProductCategory;
import ru.yandex.practicum.dto.ProductState;
import ru.yandex.practicum.model.Product;

import java.util.List;
import java.util.UUID;

public interface ProductRepository extends JpaRepository<Product, UUID> {

    List<Product> findByState(ProductState state);

    Page<Product> findByState(ProductState state, Pageable pageable);

    List<Product> findByCategoryAndState(ProductCategory category, ProductState state);

    Page<Product> findByCategoryAndState(ProductCategory category, ProductState state, Pageable pageable);
}