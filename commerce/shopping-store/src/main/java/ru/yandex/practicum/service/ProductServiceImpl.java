package ru.yandex.practicum.service;

import org.springframework.stereotype.Service;
import ru.yandex.practicum.dto.ProductDto;
import ru.yandex.practicum.enums.ProductAvailability;
import ru.yandex.practicum.enums.ProductCategory;
import ru.yandex.practicum.enums.ProductStatus;
import ru.yandex.practicum.model.Product;
import ru.yandex.practicum.repository.ProductRepository;

import java.util.List;

@Service
public class ProductServiceImpl implements ProductService {

    private final ProductRepository repository;

    public ProductServiceImpl(ProductRepository repository) {
        this.repository = repository;
    }

    @Override
    public List<ProductDto> getProducts(ProductCategory category) {
        List<Product> products;
        if (category != null) {
            products = repository.findByCategoryAndStatus(category, ProductStatus.ACTIVE);
        } else {
            products = repository.findByStatus(ProductStatus.ACTIVE);
        }
        return products.stream().map(this::toDto).toList();
    }

    @Override
    public ProductDto getProduct(Long id) {
        Product product = repository.findById(id)
                .orElseThrow(() -> new RuntimeException("Product not found"));
        return toDto(product);
    }

    @Override
    public ProductDto addProduct(ProductDto dto) {
        Product product = new Product();
        product.setName(dto.getName());
        product.setDescription(dto.getDescription());
        product.setCategory(dto.getCategory());
        product.setAvailability(dto.getAvailability());
        product.setImageSrc(dto.getImageSrc());
        product.setPrice(dto.getPrice());
        product.setStatus(ProductStatus.ACTIVE);
        return toDto(repository.save(product));
    }

    @Override
    public ProductDto updateProduct(ProductDto dto) {
        Product product = repository.findById(dto.getId())
                .orElseThrow(() -> new RuntimeException("Product not found"));
        product.setName(dto.getName());
        product.setDescription(dto.getDescription());
        product.setCategory(dto.getCategory());
        product.setAvailability(dto.getAvailability());
        product.setImageSrc(dto.getImageSrc());
        product.setPrice(dto.getPrice());
        return toDto(repository.save(product));
    }

    @Override
    public void deactivateProduct(Long id) {
        Product product = repository.findById(id)
                .orElseThrow(() -> new RuntimeException("Product not found"));
        product.setStatus(ProductStatus.DEACTIVATE);
        repository.save(product);
    }

    @Override
    public void updateQuantityState(Long id, ProductAvailability availability) {
        Product product = repository.findById(id)
                .orElseThrow(() -> new RuntimeException("Product not found"));
        product.setAvailability(availability);
        repository.save(product);
    }

    private ProductDto toDto(Product product) {
        ProductDto dto = new ProductDto();
        dto.setId(product.getId());
        dto.setName(product.getName());
        dto.setDescription(product.getDescription());
        dto.setCategory(product.getCategory());
        dto.setAvailability(product.getAvailability());
        dto.setStatus(product.getStatus());
        dto.setImageSrc(product.getImageSrc());
        dto.setPrice(product.getPrice());
        return dto;
    }
}