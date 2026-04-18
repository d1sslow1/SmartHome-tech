package ru.yandex.practicum.controller;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.ProductDto;
import ru.yandex.practicum.dto.ProductsPageResponse;
import ru.yandex.practicum.enums.ProductCategory;
import ru.yandex.practicum.enums.ProductAvailability;
import ru.yandex.practicum.service.ProductService;

@RestController
@RequestMapping("/shopping-store")
public class ProductController {

    private final ProductService productService;

    public ProductController(ProductService productService) {
        this.productService = productService;
    }

    @GetMapping("/{id}")
    public ProductDto getProduct(@PathVariable Long id) {
        return productService.getProduct(id);
    }

    @GetMapping
    public ProductsPageResponse getProducts(
            @RequestParam(required = false) ProductCategory category,
            @RequestParam(defaultValue = "0") Integer page,
            @RequestParam(defaultValue = "20") Integer size,
            @RequestParam(required = false) String[] sort) {

        Sort sorting = Sort.unsorted();
        boolean sorted = false;
        String direction = "ASC";
        String property = "productName";

        if (sort != null && sort.length > 0) {
            sorted = true;
            String sortParam = sort[0];
            if (sortParam != null && sortParam.contains(",")) {
                String[] parts = sortParam.split(",");
                property = parts[0];
                if (parts.length > 1) {
                    direction = parts[1].toUpperCase();
                }
            } else {
                property = sortParam;
            }

            Sort.Direction sortDirection = "DESC".equals(direction) ?
                    Sort.Direction.DESC : Sort.Direction.ASC;
            sorting = Sort.by(sortDirection, property);
        }

        Pageable pageable = PageRequest.of(page, size, sorting);
        Page<ProductDto> productPage = productService.getProductsPage(category, pageable);

        return new ProductsPageResponse(
                productPage.getContent(),
                productPage.getNumber(),
                productPage.getSize(),
                productPage.getTotalElements(),
                productPage.getTotalPages(),
                sorted,
                direction,
                property
        );
    }

    @PutMapping
    public ProductDto addOrUpdateProduct(@RequestBody ProductDto product) {
        if (product.getId() == null) {
            return productService.addProduct(product);
        } else {
            return productService.updateProduct(product);
        }
    }

    @PostMapping
    public ProductDto addProductPost(@RequestBody ProductDto product) {
        return productService.addProduct(product);
    }

    @PostMapping("/removeProductFromStore")
    public boolean removeProductFromStore(@RequestBody String productId) {
        try {
            Long id = Long.parseLong(productId.replace("\"", ""));
            productService.deactivateProduct(id);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @PostMapping("/quantityState")
    public boolean setQuantityState(@RequestParam Long productId,
                                    @RequestParam ProductAvailability quantityState) {
        productService.updateQuantityState(productId, quantityState);
        return true;
    }
}