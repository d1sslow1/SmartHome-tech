package ru.yandex.practicum.service;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.dto.ProductDto;
import ru.yandex.practicum.enums.ProductAvailability;
import ru.yandex.practicum.enums.ProductCategory;
import ru.yandex.practicum.enums.ProductStatus;
import ru.yandex.practicum.model.Product;
import ru.yandex.practicum.repository.ProductRepository;

import java.util.List;

@Service
@Transactional
public class ProductServiceImpl implements ProductService {

    private final ProductRepository repository;

    public ProductServiceImpl(ProductRepository repository) {
        this.repository = repository;
    }

    @Override
    @Transactional(readOnly = true)
    public Page<ProductDto> getProductsPage(ProductCategory category, Pageable pageable) {
        Page<Product> products;
        if (category != null) {
            products = repository.findByCategory(category, pageable);
        } else {
            products = repository.findAll(pageable);
        }
        return products.map(this::toDto);
    }

    @Override
    @Transactional(readOnly = true)
    public List<ProductDto> getProductsList(ProductCategory category) {
        List<Product> products;
        if (category != null) {
            products = repository.findByCategory(category);
        } else {
            products = repository.findAll();
        }
        return products.stream().map(this::toDto).toList();
    }

    @Override
    @Transactional(readOnly = true)
    public ProductDto getProduct(Long id) {
        Product product = repository.findById(id)
                .orElseThrow(() -> new RuntimeException("Product not found: " + id));
        return toDto(product);
    }

    @Override
    public ProductDto addProduct(ProductDto dto) {
        Product product = new Product();
        product.setProductName(dto.getName());
        product.setDescription(dto.getDescription());
        product.setCategory(dto.getCategory());
        product.setAvailability(dto.getAvailability());
        product.setImageSrc(dto.getImageSrc());
        product.setPrice(dto.getPrice());
        product.setStatus(dto.getStatus() != null ? dto.getStatus() : ProductStatus.ACTIVE);
        return toDto(repository.save(product));
    }

    @Override
    public ProductDto updateProduct(ProductDto dto) {
        Product product = repository.findById(dto.getId())
                .orElseThrow(() -> new RuntimeException("Product not found: " + dto.getId()));
        product.setProductName(dto.getName());
        product.setDescription(dto.getDescription());
        product.setCategory(dto.getCategory());
        product.setAvailability(dto.getAvailability());
        product.setImageSrc(dto.getImageSrc());
        product.setPrice(dto.getPrice());
        if (dto.getStatus() != null) {
            product.setStatus(dto.getStatus());
        }
        return toDto(repository.save(product));
    }

    @Override
    public void deactivateProduct(Long id) {
        Product product = repository.findById(id)
                .orElseThrow(() -> new RuntimeException("Product not found: " + id));
        product.setStatus(ProductStatus.DEACTIVATE);
        repository.save(product);
    }

    @Override
    public void updateQuantityState(Long id, ProductAvailability availability) {
        Product product = repository.findById(id)
                .orElseThrow(() -> new RuntimeException("Product not found: " + id));
        product.setAvailability(availability);
        repository.save(product);
    }

    private ProductDto toDto(Product product) {
        ProductDto dto = new ProductDto();
        dto.setId(product.getId());
        dto.setName(product.getProductName());
        dto.setDescription(product.getDescription());
        dto.setCategory(product.getCategory());
        dto.setAvailability(product.getAvailability());
        dto.setStatus(product.getStatus());
        dto.setImageSrc(product.getImageSrc());
        dto.setPrice(product.getPrice());
        return dto;
    }
}